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
 * State machine for the Token Ring cluster management protocol.
 */
public enum TokenRingState
        implements State<TokenRingContext, TokenRingMessage, TokenRingState>
{
    start
            {
                public TokenRingState handle( TokenRingContext context,
                                                                         Message message,
                                                                         MessageProcessor outgoing
                ) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch(messageType)
                    {
                        // Method call
                        case joinRing:
                            outgoing.process( broadcast( discoverRing ) );
                            return joiningRing;

                        default:
                            if (!message.hasHeader( Message.TO ))
                                outgoing.process( internal( failure, new IllegalStateException( "Not a member of a ring" ) ) );
                            return this;
                    }
                }
            },

    joiningRing
            {
                public TokenRingState handle( TokenRingContext context,
                                                                         Message message,
                                                                         MessageProcessor outgoing
                ) throws Throwable
                {
                    TokenRingMessage messageType = (TokenRingMessage) message.getMessageType();
                    switch (messageType)
                    {
                        case joining:
                        {
                            RingParticipant from = new RingParticipant(message.getHeader( FROM ));
                            context.addStandbyMonitor( from );
                            return this;
                        }
                    
                        case ringDiscovered:
                        {
                            context.setNeighbours((RingNeighbours)message.getPayload());
                            return slave;
                        }

                        case ringDiscoveryTimedOut:
                        {
                            // Check if we found other joining participants
                            List<RingParticipant> otherJoining = new ArrayList<RingParticipant>(context.getStandbyMonitors());
                            context.clearStandbyMonitors();
                            if (otherJoining.isEmpty())
                            {
                                context.setNeighbours(context.getMe(), context.getMe());
                                return master;
                            } else
                            {
                                // Find winner
                                otherJoining.add( context.getMe() );
                                Collections.sort( otherJoining, context.getParticipantComparator() );
                                if (otherJoining.get( 0 ).equals( context.getMe() ))
                                {
                                    // I won - create ring as master and invite others
                                    for(int i = 1; i < otherJoining.size(); i++)
                                    {
                                        RingNeighbours ringNeighbours = new RingNeighbours( otherJoining.get( (i+1)%otherJoining.size() ), otherJoining.get( i-1 ) );
                                        outgoing.process( Message.to( ringDiscovered, otherJoining.get( i ), ringNeighbours ) );
                                    }

                                    context.setNeighbours( otherJoining.get( 1 ), otherJoining.get( otherJoining.size()-1 ) );

                                    return master;
                                } else
                                    return this; // This is scary - what if we never get a ringDiscovered from the master? Probably need another timeout here
                            }
                        }
                        
                        case discoverRing:
                        {
                            // Found other participant that is also joining right now
                            RingParticipant from = new RingParticipant(message.getHeader( FROM ));
                            context.addStandbyMonitor( from );
                            outgoing.process( Message.to( joining, from, new RingNeighbours(context.getMe(), context.getMe()) ) );

                            return this;
                        }

                        default:
                            return this;
                    }
                }
            },

    master
            {
                public TokenRingState handle( TokenRingContext context,
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
                public TokenRingState handle( TokenRingContext context,
                                                                         Message message,
                                                                         MessageProcessor outgoing
                ) throws Throwable
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
