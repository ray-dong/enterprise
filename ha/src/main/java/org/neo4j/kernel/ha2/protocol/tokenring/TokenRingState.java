/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.ha2.protocol.tokenring;

import org.neo4j.kernel.ha2.protocol.RingNeighbours;
import org.neo4j.kernel.ha2.protocol.RingParticipant;

import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.*;

import org.neo4j.kernel.ha2.statemachine.message.BroadcastMessage;
import org.neo4j.kernel.ha2.statemachine.message.Message;
import org.neo4j.kernel.ha2.statemachine.message.MessageFrom;
import org.neo4j.kernel.ha2.statemachine.message.TargetedMessage;
import org.neo4j.kernel.ha2.statemachine.State;

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

                        default:
                            return this;
                    }
                }
            }
}
