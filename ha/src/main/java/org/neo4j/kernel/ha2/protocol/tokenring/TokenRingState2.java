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
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.State;

import static org.neo4j.com2.message.Message.*;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage2.*;

/**
 * State machine for the Token Ring cluster management protocol. This is based on the Token Ring Protocol as
 * defined here: http://www.techfest.com/networking/lan/token.htm#3.0
 */
public enum TokenRingState2
        implements State<TokenRingContext, TokenRingMessage2, TokenRingState2>
{
    start
        {
            public TokenRingState2 handle( TokenRingContext context,
                                                                     Message<TokenRingMessage2> message,
                                                                     MessageProcessor outgoing
            ) throws Throwable
            {
                switch(message.getMessageType())
                {
                    // Method call
                    case joinRing:
                        context.clearStandbyMonitors();
                        outgoing.process( broadcast( TokenRingMessage2.attachment_timer ) );
                        return monitor_check;

                    default:
                        if (!message.hasHeader( Message.TO ))
                            outgoing.process( internal( failure, new IllegalStateException( "Not a member of a ring" ) ) );
                        return this;
                }
            }
        },

    monitor_check
        {
            @Override
            public TokenRingState2 handle( TokenRingContext context,
                                                                     Message<TokenRingMessage2> message,
                                                                     MessageProcessor outgoing
            )
                throws Throwable
            {
                RingParticipant from = new RingParticipant( message.getHeader( Message.FROM ) );
                switch( message.getMessageType() )
                {
                    case active_monitor_present:
                    {
                        context.newAfter( from );
                        outgoing.process( to( standby_monitor_present, from ));
                        return neighbor_notification;
                    }

                    case standby_monitor_present:
                    {
                        // Add to list of seen standby monitors - but wait for the active monitor if possible
                        context.addStandbyMonitor( from );
                        return this;
                    }

                    case ring_purge:
                    {
                        // TODO
                        return this;
                    }

                    case attachment_timed_out:
                    {
                        // Check standby monitors first
                        RingParticipant insertionPoint = context.getRingInsertionPoint();
                        if (insertionPoint != null)
                        {
                            context.newAfter( insertionPoint );
                            outgoing.process( to( standby_monitor_present, insertionPoint ) );
                            return neighbor_notification;
                        }

                        // No standby monitors found - start our own ring
                        // TODO: this skips the Claim Token process - is it relevant in our case?
                        context.claimedToken();
                        return active_monitor;
                    }
                }

                return this;
            }
        },

    transmit_beacon
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {

                }

                return this;
            }
        },

    repeat_beacon
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {

                }

                return this;
            }
        },


    neighbor_notification
        {
            @Override
            public TokenRingState2 handle( TokenRingContext context,
                                                                     Message<TokenRingMessage2> message,
                                                                     MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {
                    case standby_monitor_present:
                    {

                        return standby_monitor;
                    }
                }

                return this;
            }
        },

    request_initialization
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {

                }

                return this;
            }
        },

    active_monitor
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch(message.getMessageType())
                {
                    case standby_monitor_present:
                    {
                        // Ring is ok - send heartbeat again!
                        outgoing.process( Message.to( active_monitor_present, tokenRingContext.getNeighbours().getAfter() ) );
                        return this;
                    }
                }
                return this;
            }
        },

    standby_monitor
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case active_monitor_present:
                    {
                        tokenRingContext.newBefore( new RingParticipant( message.getHeader( FROM ) ) );
                        outgoing.process( message.copyHeadersTo(Message.to( standby_monitor_present, tokenRingContext.getNeighbours().getAfter() ), CREATED_BY ));

                        return this;
                    }

                    case standby_monitor_present:
                    {
                        tokenRingContext.newBefore( new RingParticipant( message.getHeader( FROM ) ) );
                        outgoing.process( message.copyHeadersTo(Message.to( standby_monitor_present, tokenRingContext.getNeighbours().getAfter() ), CREATED_BY ));

                        return this;
                    }
                }


                return this;
            }
        },

    transmit_claim_token
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {

                }

                return this;
            }
        },

    repeat_claim_token
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {

                }

                return this;
            }
        },

    transmit_ring_purge
        {
            @Override
            public TokenRingState2 handle( TokenRingContext tokenRingContext,
                                                                      Message<TokenRingMessage2> message,
                                                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {

                }

                return this;
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
