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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.RingNeighbours;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.State;

import static org.neo4j.com2.message.Message.*;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage.*;

/**
 * TODO
 */
public enum TokenRingState
        implements State<TokenRingContext, TokenRingMessage>
{
    start
            {
                public State<TokenRingContext, TokenRingMessage> receive( TokenRingContext context,
                                                                          Message message,
                                                                          MessageProcessor outgoing
                ) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch(messageType)
                    {
                        // Method call
                        case start:
                            outgoing.process( broadcast( discoverRing ) );
                            return initial;

                        default:
                            if (!message.hasHeader( Message.TO ))
                                outgoing.process( internal( failure, new IllegalStateException( "Not a member of a ring" ) ) );
                            return this;
                    }
                }
            },

    initial
            {
                public State<TokenRingContext, TokenRingMessage> receive( TokenRingContext context,
                                                                          Message message,
                                                                          MessageProcessor outgoing
                ) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch (messageType)
                    {
                        case ringDiscovered:
                        {
                            context.setNeighbours((RingNeighbours)message.getPayload());
                            return slave;
                        }

                        case discoveryTimedOut:
                        {
                            context.setNeighbours(context.getMe(), context.getMe());
                            return master;
                        }

                        case discoverRing:
                        {
                            RingParticipant from = new RingParticipant(message.getHeader( FROM ));
                            if ( from.getServerId().compareTo( context.getMe().getServerId() )>0)
                            {
                                // We're both looking for ring but this server has higher server id so wins
                                // and switches to master mode

                                context.setNeighbours(from, from);
                                
                                outgoing.process( Message.to( ringDiscovered, from, new RingNeighbours(context.getMe(), context.getMe()) ) );
                                
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
                public State<TokenRingContext, TokenRingMessage> receive( TokenRingContext context,
                                                                          Message message,
                                                                          MessageProcessor outgoing
                ) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch (messageType)
                    {
                        // Method calls
                        case leaveRing:
                        {
                            if (!context.isAlone())
                            {
                                outgoing.process( Message.to( newAfter, context.getNeighbours()
                                    .getBefore(), context.getNeighbours().getAfter() ) );
                                outgoing.process( Message.to( becomeMaster, context.getNeighbours()
                                    .getAfter(), context.getNeighbours().getBefore() ) );
                            }
                            return start;
                        }

                        case sendToken:
                        {
                            outgoing.process( Message.to( becomeMaster, context.getNeighbours().getAfter() ) );
                            return slave;
                        }

                        case getParticipants:
                        case getRingParticipants:
                        case getRingParticipantsResponse:
                            getParticipants( context, message, outgoing );
                            return this;

                        // Implementation messages
                        case discoverRing:
                        {
                            RingParticipant from = new RingParticipant(message.getHeader( FROM ));
                            
                            if (!context.isAlone())
                                outgoing.process( Message.to( newAfter, context.getNeighbours().getBefore(), from ) );

                            outgoing.process( Message.to( ringDiscovered, from, new RingNeighbours( context.getNeighbours()
                                                                                                        .getBefore(), context.getMe() ) ) );

                            context.setNeighbours( from,
                                                   context.getNeighbours()
                                                       .getAfter()
                                                       .equals( context.getMe() ) ? from : context.getNeighbours()
                                                       .getAfter() );

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

                        case failure:
                            outgoing.process( message ); // Don't try to handle this

                        default:
                            return this;
                    }
                }
            },

    slave
            {
                public State<TokenRingContext, TokenRingMessage> receive( TokenRingContext context, Message message, MessageProcessor outgoing ) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch ( messageType )
                    {
                        // Method calls
                        case leaveRing:
                        {
                            if (!context.isAlone())
                            {
                                outgoing.process( Message.to( newAfter, context.getNeighbours()
                                    .getBefore(), context.getNeighbours().getAfter() ) );
                                outgoing.process( Message.to( newBefore, context.getNeighbours()
                                    .getAfter(), context.getNeighbours().getBefore() ) );
                            }

                            return start;
                        }

                        case getParticipants:
                        case getRingParticipants:
                        case getRingParticipantsResponse:
                            getParticipants( context, message, outgoing );
                            return this;

                        // Implementation messages
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

                        case failure:
                            outgoing.process( message ); // Don't try to handle this

                        default:
                            return this;
                    }
                }
            };

    private static void getParticipants( TokenRingContext context, Message message, MessageProcessor outgoing )
    {
        TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
        switch ( messageType )
        {
            case getParticipants:
            {
                if (context.isAlone())
                {
                    outgoing.process( Message.internal( getRingParticipantsResponse, Collections.singleton( context.getMe() ) ) );
                } else
                {
                    List<RingParticipant> participants = new ArrayList<RingParticipant>(  );
                    participants.add( context.getMe() );
                    outgoing.process( Message.to( getRingParticipants, context.getNeighbours().getAfter(), participants ) );
                }
                return;
            }

            case getRingParticipants:
            case getRingParticipantsResponse:
            {
                if (context.isConversationCreatedByMe( message ))
                {
                    // We're done
                    outgoing.process( Message.internal( getRingParticipantsResponse, message.getPayload() ) );
                } else
                {
                    List<RingParticipant> participants = (List<RingParticipant>) message.getPayload();
                    participants.add( context.getMe() );
                    outgoing.process( Message.to( getRingParticipantsResponse, context.getNeighbours().getAfter(), participants ) );
                }
                return;
            }
        }
    }
}
