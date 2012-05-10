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

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

import static org.neo4j.com_2.message.Message.*;

/**
 * TODO
 */
public enum HeartbeatStateX
    implements State<HeartbeatContextX, HeartbeatMessageX>
{
    start
    {
        @Override
        public HeartbeatStateX handle( HeartbeatContextX context,
                                             Message<HeartbeatMessageX> message,
                                             MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case possibleServers:
                {
                    context.setPossibleServers( (String[]) message.getPayload() );
                    break;
                }

                case addHeartbeatListener:
                {
                    context.addHeartbeatListener((HeartbeatListenerX) message.getPayload());
                    break;
                }

                case removeHeartbeatListener:
                {
                    context.removeHeartbeatListener( (HeartbeatListenerX) message.getPayload());
                    break;
                }

                case join:
                {
                    // Setup heartbeat timeouts
                    for( String server : context.servers )
                    {
                        if (!context.me.equals( server ))
                            context.timeouts.setTimeout( server, internal( HeartbeatMessageX.timed_out, server ) );
                    }

                    // Send first heartbeat
                    outgoing.process( internal( HeartbeatMessageX.send_heartbeat ) );

                    return running;
                }
            }

            return this;
        }
    },

    running
    {
        @Override
        public HeartbeatStateX handle( HeartbeatContextX context,
                                             Message<HeartbeatMessageX> message,
                                             MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case i_am_alive:
                {
                    HeartbeatMessageX.IAmAliveState state = (HeartbeatMessageX.IAmAliveState) message.getPayload();

                    context.alive( state.getServer() );

                    if (!context.me.equals( state.getServer() ))
                    {
                        context.timeouts.cancelTimeout( state.getServer() );
                        context.timeouts.setTimeout( state.getServer(), internal( HeartbeatMessageX.timed_out, state.getServer() ) );
                    }

                    break;
                }

                case timed_out:
                {
                    String server = (String) message.getPayload();

                    context.failed( server );

                    context.timeouts.setTimeout( server, internal( HeartbeatMessageX.timed_out, server ) );
                    break;
                }

                case send_heartbeat:
                {
                    // Send heartbeat message to all other servers
                    for( String server : context.servers )
                    {
                        outgoing.process( to( HeartbeatMessageX.i_am_alive, server, new HeartbeatMessageX.IAmAliveState( context.me ) ) );
                    }

                    context.timeouts.setTimeout( context.me, internal( HeartbeatMessageX.send_heartbeat ) );
                    break;
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
