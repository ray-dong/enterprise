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
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
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
import org.neo4j.kernel.ha2.protocol.cluster.Cluster;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterConfiguration;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterMessage;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterState;
import org.neo4j.kernel.ha2.protocol.election.ClusterLeaveReelectionListener;
import org.neo4j.kernel.ha2.protocol.election.Election;
import org.neo4j.kernel.ha2.protocol.election.ElectionContext;
import org.neo4j.kernel.ha2.protocol.election.ElectionCredentialsProvider;
import org.neo4j.kernel.ha2.protocol.election.ElectionMessage;
import org.neo4j.kernel.ha2.protocol.election.ElectionRole;
import org.neo4j.kernel.ha2.protocol.election.ElectionState;
import org.neo4j.kernel.ha2.protocol.election.HeartbeatFailedReelectionListener;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatContext;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatIAmAliveProcessor;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatJoinListener;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatRefreshProcessor;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatState;
import org.neo4j.kernel.ha2.protocol.snapshot.SnapshotContext;
import org.neo4j.kernel.ha2.protocol.snapshot.SnapshotMessage;
import org.neo4j.kernel.ha2.protocol.snapshot.SnapshotState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.statemachine.StateMachineRules;
import org.neo4j.kernel.ha2.timeout.LatencyCalculator;
import org.neo4j.kernel.ha2.timeout.TimeoutStrategy;
import org.neo4j.kernel.ha2.timeout.Timeouts;

import static org.neo4j.com_2.message.Message.*;
import static org.neo4j.helpers.collection.Iterables.*;

/**
 * TODO
 */
public class MultiPaxosServerFactory
    implements ProtocolServerFactory
{
    private ClusterConfiguration initialConfig;

    public MultiPaxosServerFactory(ClusterConfiguration initialConfig)
    {
        this.initialConfig = initialConfig;
    }

    @Override
    public ProtocolServer newProtocolServer(TimeoutStrategy timeoutStrategy, MessageSource input, MessageProcessor output,
                                            AcceptorInstanceStore acceptorInstanceStore, ElectionCredentialsProvider electionCredentialsProvider )
    {
        LatencyCalculator latencyCalculator = new LatencyCalculator(timeoutStrategy, input);

        // Create state machines
        ConnectedStateMachines connectedStateMachines = new ConnectedStateMachines( input, output, latencyCalculator );
        Timeouts timeouts = connectedStateMachines.getTimeouts();
        connectedStateMachines.addMessageProcessor( latencyCalculator );

        AcceptorContext acceptorContext = new AcceptorContext( acceptorInstanceStore );
        LearnerContext learnerContext = new LearnerContext();
        ProposerContext proposerContext = new ProposerContext();
        final ClusterContext clusterContext = new ClusterContext(proposerContext, learnerContext, new ClusterConfiguration( initialConfig.getName(), initialConfig.getNodes() ),timeouts);
        final HeartbeatContext heartbeatContext = new HeartbeatContext(clusterContext, learnerContext);
        final MultiPaxosContext context = new MultiPaxosContext(clusterContext, proposerContext, learnerContext, heartbeatContext, timeouts);
        ElectionContext electionContext = new ElectionContext( iterable( new ElectionRole( ClusterConfiguration.COORDINATOR ) ), clusterContext, heartbeatContext );
        SnapshotContext snapshotContext = new SnapshotContext( clusterContext, learnerContext );
        AtomicBroadcastContext atomicBroadcastContext = new AtomicBroadcastContext( clusterContext );

        connectedStateMachines.addStateMachine( new StateMachine( atomicBroadcastContext, AtomicBroadcastMessage.class, AtomicBroadcastState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(acceptorContext, AcceptorMessage.class, AcceptorState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(context, ProposerMessage.class, ProposerState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(context, LearnerMessage.class, LearnerState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(heartbeatContext, HeartbeatMessage.class, HeartbeatState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(electionContext, ElectionMessage.class, ElectionState.start) );
        connectedStateMachines.addStateMachine( new StateMachine(snapshotContext, SnapshotMessage.class, SnapshotState.start) );

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

        connectedStateMachines.addMessageProcessor( new HeartbeatRefreshProcessor( connectedStateMachines.getOutgoing() ) );
        input.addMessageProcessor( new HeartbeatIAmAliveProcessor( connectedStateMachines.getOutgoing() ) );

        server.newClient( Cluster.class ).addClusterListener( new HeartbeatJoinListener(connectedStateMachines.getOutgoing()));

        heartbeatContext.addHeartbeatListener( new HeartbeatFailedReelectionListener(server.newClient( Election.class )) );
        clusterContext.addClusterListener( new ClusterLeaveReelectionListener(server.newClient( Election.class )) );
        electionContext.setElectionCredentialsProvider( electionCredentialsProvider );

        StateMachineRules rules = new StateMachineRules( connectedStateMachines.getOutgoing() )
           .rule( ClusterState.start, ClusterMessage.create, ClusterState.entered,
                  internal( AtomicBroadcastMessage.entered ),
                  internal( ProposerMessage.join ),
                  internal( AcceptorMessage.join ),
                  internal( LearnerMessage.join ),
                  internal( HeartbeatMessage.join ),
                  internal( ElectionMessage.created ),
                  internal( SnapshotMessage.join ) )

           .rule( ClusterState.acquiringConfiguration, ClusterMessage.configurationResponse, ClusterState.joining,
                  internal( AcceptorMessage.join ),
                  internal( LearnerMessage.join ),
                  internal( AtomicBroadcastMessage.join ) )

           .rule( ClusterState.acquiringConfiguration, ClusterMessage.configurationResponse, ClusterState.entered,
                  internal( AtomicBroadcastMessage.entered ),
                  internal( ProposerMessage.join ),
                  internal( AcceptorMessage.join ),
                  internal( LearnerMessage.join ),
                  internal( HeartbeatMessage.join ),
                  internal( ElectionMessage.join ),
                  internal( SnapshotMessage.join ) )

           .rule( ClusterState.joining, ClusterMessage.configurationChanged, ClusterState.entered,
                  internal( AtomicBroadcastMessage.entered ),
                  internal( ProposerMessage.join ),
                  internal( AcceptorMessage.join ),
                  internal( LearnerMessage.join ),
                  internal( HeartbeatMessage.join ),
                  internal( ElectionMessage.join ),
                  internal( SnapshotMessage.join ) )

           .rule( ClusterState.joining, ClusterMessage.joinFailure, ClusterState.start,
                  internal( AtomicBroadcastMessage.leave ),
                  internal( AcceptorMessage.leave ),
                  internal( LearnerMessage.leave ),
                  internal( ProposerMessage.leave ) )

           .rule( ClusterState.entered, ClusterMessage.leave, ClusterState.start,
                  internal( AtomicBroadcastMessage.leave ),
                  internal( AcceptorMessage.leave ),
                  internal( LearnerMessage.leave ),
                  internal( HeartbeatMessage.leave ),
                  internal( SnapshotMessage.leave ),
                  internal( ElectionMessage.leave ),
                  internal( ProposerMessage.leave ) )

           .rule( ClusterState.entered, ClusterMessage.leave, ClusterState.start,
                  internal( AtomicBroadcastMessage.leave ),
                  internal( AcceptorMessage.leave ),
                  internal( LearnerMessage.leave ),
                  internal( HeartbeatMessage.leave ),
                  internal( ElectionMessage.leave ),
                  internal( SnapshotMessage.leave ),
                  internal( ProposerMessage.leave ) )

           .rule( ClusterState.leaving, ClusterMessage.configurationChanged, ClusterState.start,
                  internal( AtomicBroadcastMessage.leave ),
                  internal( AcceptorMessage.leave ),
                  internal( LearnerMessage.leave ),
                  internal( HeartbeatMessage.leave ),
                  internal( ElectionMessage.leave ),
                  internal( SnapshotMessage.leave ),
                  internal( ProposerMessage.leave ) );



        connectedStateMachines.addStateTransitionListener(rules);

        return server;
    }
}
