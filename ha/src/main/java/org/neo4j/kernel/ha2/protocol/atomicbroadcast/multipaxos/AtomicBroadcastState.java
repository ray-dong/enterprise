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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos;

import java.util.concurrent.TimeoutException;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.Payload;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterMessage;
import org.neo4j.kernel.ha2.statemachine.State;

import static org.neo4j.com_2.message.Message.*;

/**
 * State Machine for implementation of Atomic Broadcast client interface
 */
public enum AtomicBroadcastState
    implements State<AtomicBroadcastContext, AtomicBroadcastMessage>
{
    start
        {
            @Override
            public AtomicBroadcastState handle( AtomicBroadcastContext context,
                                      Message<AtomicBroadcastMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {

                switch( message.getMessageType() )
                {
                    case entered:
                    {
                        return broadcasting;
                    }

                    case join:
                    {
                        return joining;
                    }

                    default:
                    {
                        defaultHandling(context, message, outgoing);
                    }
                }

                return this;
            }
        },

    joining
        {
            @Override
            public State<?, ?> handle( AtomicBroadcastContext context,
                                       Message<AtomicBroadcastMessage> message,
                                       MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case failed:
                    {
                        // Joining failed
                        outgoing.process( internal( ClusterMessage.joinFailure, new TimeoutException( "Could not join cluster" ) ) );

                        return start;
                    }

                    case broadcastResponse:
                    {
                        if (message.getPayload() instanceof ClusterMessage.ConfigurationChangeState)
                            outgoing.process( internal( ClusterMessage.configurationChanged, message.getPayload() ) );

                        break;
                    }

                    case entered:
                    {
                        return broadcasting;
                    }

                    default:
                    {
                        defaultHandling(context, message, outgoing);
                    }
                }

                return this;
            }
        },

    broadcasting
        {
            @Override
            public AtomicBroadcastState handle( AtomicBroadcastContext context,
                                      Message<AtomicBroadcastMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch (message.getMessageType())
                {
                    case broadcast:
                    {
                        outgoing.process( internal( ProposerMessage.propose, message.getPayload() ) );
                        break;
                    }

                    case broadcastResponse:
                    {
                        if (message.getPayload() instanceof ClusterMessage.ConfigurationChangeState)
                            outgoing.process( internal( ClusterMessage.configurationChanged, message.getPayload() ) );
                        else
                            context.receive(message.<Payload>getPayload());

                        break;
                    }

                    case leave:
                    {
                        return start;
                    }

                    default:
                    {
                        defaultHandling(context, message, outgoing);
                    }
                }

                return this;
            }
        };

    private static void defaultHandling(AtomicBroadcastContext context, Message<AtomicBroadcastMessage> message, MessageProcessor outgoing)
    {
        switch (message.getMessageType())
        {
            case addAtomicBroadcastListener:
            {
                context.addAtomicBroadcastListener( (AtomicBroadcastListener) message.getPayload() );
                break;
            }

            case removeAtomicBroadcastListener:
            {
                context.removeAtomicBroadcastListener( (AtomicBroadcastListener) message.getPayload() );
                break;
            }
        }
    }
}
