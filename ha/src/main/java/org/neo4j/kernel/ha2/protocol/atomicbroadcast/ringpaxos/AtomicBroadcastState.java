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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.kernel.ha2.statemachine.State;

import static org.neo4j.com_2.message.Message.internal;

/**
 * State Machine for implementation of Atomic Broadcast client interface
 */
public enum AtomicBroadcastState
    implements State<RingPaxosContext, AtomicBroadcastMessage>
{
    start
        {
            @Override
            public AtomicBroadcastState handle( RingPaxosContext context,
                                      Message<AtomicBroadcastMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {

                switch( message.getMessageType() )
                {
                    case possibleServers:
                    {
                        context.setPossibleServers( (String[]) message.getPayload() );
                        context.acceptors.clear();
                        for( String serverId : context.getPossibleServers() )
                        {
                            context.acceptors.add( serverId );
                        }
                        context.learners.clear();
                        for( String serverId : context.getPossibleServers() )
                        {
                            context.learners.add( serverId );
                        }
                        context.coordinator = context.getAcceptors().get( 0 ); // TODO Do formal election
                        break;
                    }

                    case join:
                    {
                        outgoing.process( internal( AcceptorMessage.join ) );
                        outgoing.process( internal( CoordinatorMessage.join ) );
                        outgoing.process( internal( LearnerMessage.join ) );

                        outgoing.process( internal( AtomicBroadcastMessage.joined ) );
                        return joined;
                    }
                }

                return this;
            }
        },

    joined
        {
            @Override
            public AtomicBroadcastState handle( RingPaxosContext context,
                                      Message<AtomicBroadcastMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {
                    case addPaxosListener:
                    {
                        context.addPaxosListener( (AtomicBroadcastListener) message.getPayload());
                        break;
                    }

                    case removePaxosListener:
                    {
                        context.removePaxosListener( (AtomicBroadcastListener) message.getPayload() );
                        break;
                    }

                    case propose:
                    {
                        // TODO This assumes that this process is coordinator. Should handle other cases as well
                        outgoing.process( internal( CoordinatorMessage.propose, message.getPayload() ) );
                        break;
                    }

                    case fail:
                    {
                        // TODO Handle notification of failure. Should recreate cluster without given node
                        String serverId = message.getPayload().toString();
                        break;
                    }

                    case recover:
                    {
                        // TODO Handle notification of recovery. Should recreate cluster with given node
                        String serverId = message.getPayload().toString();
                        break;
                    }

                    case leave:
                    {
                        return start;
                    }
                }

                return this;
            }
        }
}
