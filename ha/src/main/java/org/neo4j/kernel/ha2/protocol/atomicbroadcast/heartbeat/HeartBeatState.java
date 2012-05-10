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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat;

import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

import static org.neo4j.com2.message.Message.*;

/**
 * TODO
 */
public enum HeartBeatState
    implements State<HeartBeatContext, HeartBeatMessage, HeartBeatState>
{
    start
    {
        @Override
        public HeartBeatState handle( HeartBeatContext context,
                                             Message<HeartBeatMessage> message,
                                             MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case join:
                {
                    // Setup heartbeat timeouts
                    for( String server : context.servers )
                    {
                        context.timeouts.setTimeout( server, internal( HeartBeatMessage.timed_out, server ) );
                    }

                    // Send first heartbeat
                    outgoing.process( internal( HeartBeatMessage.send_heartbeat ) );

                    return running;
                }
            }

            return this;
        }
    },

    running
    {
        @Override
        public HeartBeatState handle( HeartBeatContext context,
                                             Message<HeartBeatMessage> message,
                                             MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case i_am_alive:
                {
                    HeartBeatMessage.IAmAliveState state = (HeartBeatMessage.IAmAliveState) message.getPayload();

                    context.alive( state.getServer() );

                    context.timeouts.cancelTimeout( state.getServer() );
                    context.timeouts.setTimeout( state.getServer(), internal( HeartBeatMessage.timed_out ) );

                    break;
                }

                case timed_out:
                {
                    String server = (String) message.getPayload();

                    context.failed.add( server );

                    context.timeouts.setTimeout( server, internal( HeartBeatMessage.timed_out ) );
                }

                case send_heartbeat:
                {
                    // Send heartbeat message to all other servers
                    for( String server : context.servers )
                    {
                        outgoing.process( to( HeartBeatMessage.i_am_alive, server, new HeartBeatMessage.IAmAliveState( context.me ) ) );
                    }

                    context.timeouts.setTimeout( context.me, internal( HeartBeatMessage.send_heartbeat ) );
                }

                case leave:
                {
                    // Cancel all existing timeouts
                    for( String server : context.servers )
                    {
                        context.timeouts.cancelTimeout( server );
                    }

                    return start;
                }
            }

            return this;
        }
    }
}
