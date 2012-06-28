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

package org.neo4j.kernel.ha2.protocol.cluster;

import static org.neo4j.com_2.message.Message.internal;
import static org.neo4j.com_2.message.Message.respond;
import static org.neo4j.com_2.message.Message.timeout;
import static org.neo4j.com_2.message.Message.to;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.kernel.ha2.statemachine.State;
import org.slf4j.LoggerFactory;

/**
 * State machine for the Cluster API
 */
public enum ClusterState
    implements State<ClusterContext,ClusterMessage>
{
    start
    {
        @Override
        public State<?, ?> handle(ClusterContext context, Message<ClusterMessage> message, MessageProcessor outgoing) throws Throwable
        {
            switch( message.getMessageType() )
            {
                case addClusterListener:
                {
                    context.addClusterListener( message.<ClusterListener>getPayload() );

                    break;
                }

                case removeClusterListener:
                {
                    context.removeClusterListener( message.<ClusterListener>getPayload() );

                    break;
                }

                case create:
                {
                    String name = message.getPayload();
                    context.created( name );
                    return entered;
                }

                case join:
                {
                    URI clusterNodeUri = message.getPayload();
                    outgoing.process( to( ClusterMessage.configuration, clusterNodeUri ) );
                    context.timeouts.setTimeout( clusterNodeUri, timeout( ClusterMessage.configurationTimeout, message ) );
                    return acquiringConfiguration;
                }
            }
            return this;
        }
    },

    acquiringConfiguration
    {
        @Override
        public State<?, ?> handle(ClusterContext context, Message<ClusterMessage> message, MessageProcessor outgoing) throws Throwable
        {
            switch(message.getMessageType())
            {
                case configurationResponse:
                {
                    context.timeouts.cancelTimeout( new URI( message.getHeader( Message.FROM )) );

                    ClusterMessage.ConfigurationResponseState state = message.getPayload();

                    List<URI> nodeList = new ArrayList<URI>(state.getNodes());
                    if (!nodeList.contains(context.me))
                    {
                        context.learnerContext.setLastDeliveredInstanceId( state.getLatestReceivedInstanceId().getId());
                        context.learnerContext.learnedInstanceId( state.getLatestReceivedInstanceId().getId());
                        context.proposerContext.lastInstanceId = state.getLatestReceivedInstanceId().getId()+1;

                        context.acquiredConfiguration( nodeList, state.getRoles() );

                        LoggerFactory.getLogger(ClusterState.class).info( "Joining:"+nodeList );

                        ClusterMessage.ConfigurationChangeState newState = new ClusterMessage.ConfigurationChangeState();
                        newState.join( context.me );

                        // Let the coordinator propose this
                        outgoing.process( to( ProposerMessage.propose, state.getRoles().get( ClusterConfiguration.COORDINATOR ), newState ));

                        // TODO timeout this

                        return joining;
                    } else
                    {
                        // TODO Already in, go to joined state
                        context.acquiredConfiguration( nodeList, state.getRoles() );

                        return entered;
                    }
                }

                case configurationTimeout:
                {
                    outgoing.process( internal( ClusterMessage.joinFailure, new TimeoutException( "Join failed, timeout waiting for configuration" ) ) );
                    // TODO
                    return joining;
                }
            }

            return this;
        }
    },

    joining
    {
        @Override
        public State<?, ?> handle( ClusterContext context,
                                   Message<ClusterMessage> message,
                                   MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case configurationChanged:
                {
                    ClusterMessage.ConfigurationChangeState state = message.getPayload();
                    // TODO Verify that this is the change we sent out in the first place

                    if (context.getMe().equals( state.getJoin() ))
                    {
                        context.joined();
                        outgoing.process( internal( ClusterMessage.joinResponse, context.getConfiguration() ) );
                        return entered;
                    } else
                    {
                        state.apply( context );
                        return this;
                    }
                }

                case joinFailure:
                {
                    return start;
                }
            }

            return this;
        }
    },

    entered
    {
        @Override
        public State<?, ?> handle(ClusterContext context, Message<ClusterMessage> message, MessageProcessor outgoing) throws Throwable
        {
            switch (message.getMessageType())
            {
                case addClusterListener:
                {
                    context.addClusterListener( message.<ClusterListener>getPayload() );

                    break;
                }

                case removeClusterListener:
                {
                    context.removeClusterListener( message.<ClusterListener>getPayload() );

                    break;
                }

                case configuration:
                {
                    outgoing.process( respond( ClusterMessage.configurationResponse, message, new ClusterMessage.ConfigurationResponseState( context.getConfiguration().getRoles(),
                                                                                                                                             context.getConfiguration().getNodes(),
                                                                                                                                             new InstanceId(context.learnerContext.getLastDeliveredInstanceId() ) )));
                    break;
                }

                case configurationChanged:
                {
                    ClusterMessage.ConfigurationChangeState state = message.getPayload();
                    state.apply( context );
                    break;
                }

                case leave:
                {
                    List<URI> nodeList = new ArrayList<URI>(context.getConfiguration().getNodes());
                    if (nodeList.size() == 1)
                    {
                        context.left();

                        return start;

                    } else
                    {
                        LoggerFactory.getLogger(ClusterState.class).info( "Leaving:" + nodeList );

                        ClusterMessage.ConfigurationChangeState newState = new ClusterMessage.ConfigurationChangeState();
                        newState.leave( context.me );

                        outgoing.process(internal( AtomicBroadcastMessage.broadcast, newState ));

                        return leaving;
                    }
                }
            }

            return this;
        }
    },

    leaving
    {
        @Override
        public State<?, ?> handle( ClusterContext context,
                                   Message<ClusterMessage> message,
                                   MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case configurationChanged:
                {
                    ClusterMessage.ConfigurationChangeState state = message.getPayload();
                    if (state.isLeaving(context.getMe()))
                    {
                        context.left();

                        return start;
                    } else
                    {
                        state.apply(context);
                    }
                }
            }

            return this;
        }
    }
}
