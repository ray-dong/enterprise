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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.kernel.ha2.statemachine.State;

import static org.neo4j.com_2.message.Message.*;

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
                    context.created();
                    outgoing.process( internal( AtomicBroadcastMessage.join ) );
                    outgoing.process( internal( AcceptorMessage.join ) );
                    outgoing.process( internal( LearnerMessage.join ) );
                    return joined;
                }

                case join:
                {
                    URI clusterNodeUri = message.getPayload();
                    outgoing.process( to( ClusterMessage.configuration, clusterNodeUri ) );
                    context.timeouts.setTimeout( clusterNodeUri, internal( ClusterMessage.configurationTimeout ) );
                    return acquiringConfiguration;
                }

                case leave:
                {
                    break;
                }

                case configuration:
                    break;
                case configurationResponse:
                    break;
                case configurationTimeout:
                    break;
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
                        context.learnerContext.lastReceivedInstanceId = state.getLatestReceivedInstanceId().getId();
                        context.proposerContext.lastInstanceId = state.getLatestReceivedInstanceId().getId()+1;

                        nodeList.add(context.me);

                        ClusterMessage.ConfigurationChangeState newState = new ClusterMessage.ConfigurationChangeState(nodeList);

                        outgoing.process( internal( AcceptorMessage.join ) );
                        outgoing.process( internal( LearnerMessage.join ) );
                        outgoing.process( internal( AtomicBroadcastMessage.join ) );
                        outgoing.process(internal( ProposerMessage.propose, newState ));

                        // TODO timeout this

                        return joining;
                    } else
                    {
                        // TODO Already in, go to joined state
                        return joined;
                    }
                }

                case configurationTimeout:
                {
                    // TODO
                    break;
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

                    context.joined( state.getNodes() );
                    outgoing.process( internal( ProposerMessage.join ) );
                    return joined;
                }
            }

            return this;
        }
    },

    joined
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
                    outgoing.process( respond( ClusterMessage.configurationResponse, message, new ClusterMessage.ConfigurationResponseState( context.getConfiguration().getNodes(),
                                                                                                                                             context.getConfiguration().getNodes(), new InstanceId(context.learnerContext.lastReceivedInstanceId ) )));
                    break;
                }

                case configurationChanged:
                {
                    ClusterMessage.ConfigurationChangeState state = message.getPayload();
                    context.joined( state.getNodes() );
                    break;
                }

                case leave:
                {
                    List<URI> nodeList = new ArrayList<URI>(context.getConfiguration().getNodes());
                    if (nodeList.size() == 1)
                    {
                        context.left();

                        outgoing.process( internal( ProposerMessage.leave ) );
                        outgoing.process( internal( AcceptorMessage.leave ) );
                        outgoing.process( internal( LearnerMessage.leave ) );
                        outgoing.process( internal( AtomicBroadcastMessage.leave ) );

                        return start;

                    } else
                    {
                        nodeList.remove(context.me);

                        ClusterMessage.ConfigurationChangeState newState = new ClusterMessage.ConfigurationChangeState(nodeList);

                        outgoing.process(internal( ProposerMessage.propose, newState ));

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
                    if (!state.getNodes().contains( context.getMe() ))
                    {
                        context.left();

                        outgoing.process( internal( ProposerMessage.leave ) );
                        outgoing.process( internal( AcceptorMessage.leave ) );
                        outgoing.process( internal( LearnerMessage.leave ) );
                        outgoing.process( internal( AtomicBroadcastMessage.leave ) );

                        return start;
                    }
                }
            }

            return this;
        }
    }
}
