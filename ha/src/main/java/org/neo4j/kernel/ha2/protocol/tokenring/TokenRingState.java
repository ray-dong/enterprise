/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha2.protocol.tokenring;

import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.becomeMaster;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.discoverRing;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.getParticipantsResponse;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.newAfter;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.newBefore;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.ringDiscovered;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.kernel.ha2.protocol.RingNeighbours;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.State;
import org.neo4j.kernel.ha2.statemachine.message.BroadcastMessage;
import org.neo4j.kernel.ha2.statemachine.message.Message;
import org.neo4j.kernel.ha2.statemachine.message.MessageFrom;

/**
 * TODO
 */
public enum TokenRingState
        implements State<TokenRingContext, TokenRingMessage>
{
    start
            {
                public State<TokenRingContext, TokenRingMessage> receive(TokenRingContext context, Message message) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch(messageType)
                    {
                        case start:
                            context.broadcast(discoverRing);
                            return initial;

                        default:
                            return this;
                    }
                }
            },

    initial
            {
                public State<TokenRingContext, TokenRingMessage> receive(TokenRingContext context, Message message) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch (messageType)
                    {
                        case ringDiscovered:
                        {
                            context.setNeighbours((RingNeighbours)message.getPayload());
                            return slave;
                        }

                        case failure:
                        {
                            context.setNeighbours(context.getMe(), context.getMe());
                            return master;
                        }

                        case discoverRing:
                        {
                            MessageFrom broadcastMessage = (MessageFrom)message;
                            if (broadcastMessage.getFrom().getServerId().compareTo(context.getMe().getServerId())>0)
                            {
                                // We're both looking for ring but this server has higher server id so wins
                                // and switches to master mode

                                context.setNeighbours(broadcastMessage.getFrom(),broadcastMessage.getFrom());
                                
                                context.send(ringDiscovered, broadcastMessage.getFrom(),
                                        new RingNeighbours(context.getMe(), context.getMe()));
                                
                                return master;
                            } else
                            {
                                return this;
                            }
                        }

                        default:
                            return this;
                    }
                }
            },

    master
            {
                public State<TokenRingContext, TokenRingMessage> receive(TokenRingContext context, Message message) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch (messageType)
                    {
                        case discoverRing:
                        {
                            BroadcastMessage broadcastMessage = (BroadcastMessage)message;
                            context.setNeighbours(broadcastMessage.getFrom(),
                                    context.getNeighbours().getAfter().equals(context.getMe())?broadcastMessage.getFrom(): context.getNeighbours().getAfter());

                            if (!context.getNeighbours().getBefore().equals(context.getMe()))
                                context.send(newAfter, context.getNeighbours().getBefore(), broadcastMessage.getFrom());
                            context.send(ringDiscovered, broadcastMessage.getFrom(),
                                    new RingNeighbours(context.getNeighbours().getBefore(), context.getMe()));

                            return this;
                        }

                        case newAfter:
                        {
                            context.newAfter((RingParticipant) message.getPayload());
                            return this;
                        }

                        case newBefore:
                        {
                            context.newBefore((RingParticipant) message.getPayload());
                            return this;
                        }

                        case leaveRing:
                        {
                            if (!context.getNeighbours().getAfter().equals(context.getMe()))
                            {
                                context.send(newAfter, context.getNeighbours().getBefore(), context.getNeighbours().getAfter());
                                context.send(becomeMaster, context.getNeighbours().getAfter(), context.getNeighbours().getBefore());
                                return start;
                            }
                        }

                        case sendToken:
                        {
                            context.send(becomeMaster, context.getNeighbours().getAfter(), null);
                            return slave;
                        }

                        case getParticipants:
                        case getParticipantsResponse:
                            getParticipants( context, message );

                        default:
                            return this;
                    }
                }
            },

    slave
            {
                public State receive(TokenRingContext context, Message message) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch ( messageType )
                    {
                        case becomeMaster:
                        {
                            if (message.getPayload() != null)
                                context.newBefore((RingParticipant) message.getPayload());

                            return master;
                        }

                        case newAfter:
                        {
                            context.newAfter((RingParticipant) message.getPayload());
                            return this;
                        }

                        case newBefore:
                        {
                            context.newBefore((RingParticipant) message.getPayload());

                            return this;
                        }

                        case leaveRing:
                        {
                            if (!context.getNeighbours().getAfter().equals(context.getMe()))
                            {
                                context.send(newAfter, context.getNeighbours().getBefore(), context.getNeighbours().getAfter());
                                context.send(newBefore, context.getNeighbours().getAfter(), context.getNeighbours().getBefore());
                            }

                            return start;
                        }
                        
                        case getParticipants:
                        case getParticipantsResponse:
                            getParticipants( context, message );

                        default:
                            return this;
                    }
                }
            };

    public void getParticipants( TokenRingContext context, Message message )
    {
        TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
        switch ( messageType )
        {
            case getParticipants:
            {
                if (!context.getNeighbours().getAfter().equals(context.getMe()))
                {
                    List<RingParticipant> participants = new ArrayList<RingParticipant>(  );
                    participants.add( context.getMe() );
                    context.send( getParticipantsResponse, context.getNeighbours().getAfter(), participants );
                } else
                {
                    context.internal( getParticipantsResponse, Collections.singleton( context.getMe() ) );
                }
                return;
            }

            case getParticipantsResponse:
            {
                if (context.getMe().toString().equals(message.getHeader( Message.CREATED_BY ) ))
                {
                    // We're done
                    context.internal( getParticipantsResponse, message.getPayload() );
                } else
                {
                    List<RingParticipant> participants = (List<RingParticipant>) message.getPayload();
                    participants.add( context.getMe() );
                    context.send( getParticipantsResponse, context.getNeighbours().getAfter(), participants );
                }
                return;
            }
        }
    }
}
