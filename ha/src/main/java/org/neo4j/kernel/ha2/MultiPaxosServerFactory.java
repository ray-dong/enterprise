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

package org.neo4j.kernel.ha2;

import java.net.URI;

import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageSource;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatIAmAliveProcessor;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatRefreshProcessor;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.MultiPaxosContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerState;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterConfiguration;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterMessage;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterState;
import org.neo4j.kernel.ha2.protocol.election.ElectionContext;
import org.neo4j.kernel.ha2.protocol.election.ElectionMessage;
import org.neo4j.kernel.ha2.protocol.election.ElectionRole;
import org.neo4j.kernel.ha2.protocol.election.ElectionState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.timeout.LatencyCalculator;
import org.neo4j.kernel.ha2.timeout.TimeoutStrategy;
import org.neo4j.kernel.ha2.timeout.Timeouts;

import static org.neo4j.helpers.collection.Iterables.iterable;

/**
 * TODO
 */
public class MultiPaxosServerFactory
    implements ProtocolServerFactory
{
    private ClusterConfiguration initialConfig;

    public MultiPaxosServerFactory()
    {
        this(new ClusterConfiguration(  ));
    }

    public MultiPaxosServerFactory(ClusterConfiguration initialConfig)
    {
        this.initialConfig = initialConfig;
    }

    @Override
    public ProtocolServer newProtocolServer(TimeoutStrategy timeoutStrategy, MessageSource input, MessageProcessor output )
    {
        LatencyCalculator latencyCalculator = new LatencyCalculator(timeoutStrategy, input);

        // Create state machines
        ConnectedStateMachines connectedStateMachines = new ConnectedStateMachines( input, output, latencyCalculator );
        Timeouts timeouts = connectedStateMachines.getTimeouts();
        connectedStateMachines.addMessageProcessor( new HeartbeatRefreshProcessor( connectedStateMachines.getOutgoing() ) );
        connectedStateMachines.addMessageProcessor( latencyCalculator );
        input.addMessageProcessor( new HeartbeatIAmAliveProcessor( connectedStateMachines.getOutgoing() ) );

        LearnerContext learnerContext = new LearnerContext();
        ProposerContext proposerContext = new ProposerContext();
        final ClusterContext clusterContext = new ClusterContext(proposerContext, learnerContext, new ClusterConfiguration( initialConfig.getNodes() ),timeouts);
        final HeartbeatContext heartbeatContext = new HeartbeatContext(clusterContext);
        final MultiPaxosContext context = new MultiPaxosContext(clusterContext, proposerContext, learnerContext, timeouts);
        ElectionContext electionContext = new ElectionContext( iterable( new ElectionRole( "coordinator" ) ), clusterContext, heartbeatContext );

        connectedStateMachines.addStateMachine( new StateMachine(new AtomicBroadcastContext(), AtomicBroadcastMessage.class, AtomicBroadcastState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(context, AcceptorMessage.class, AcceptorState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(context, ProposerMessage.class, ProposerState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(context, LearnerMessage.class, LearnerState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(heartbeatContext, HeartbeatMessage.class, HeartbeatState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(electionContext, ElectionMessage.class, ElectionState.start) );

        final ProtocolServer server = new ProtocolServer( connectedStateMachines);

        StateMachine cluster = new StateMachine(clusterContext, ClusterMessage.class, ClusterState.start);

        connectedStateMachines.addStateMachine( cluster );

        server.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                clusterContext.setMe( me );
            }
        } );

        return server;
    }
}
