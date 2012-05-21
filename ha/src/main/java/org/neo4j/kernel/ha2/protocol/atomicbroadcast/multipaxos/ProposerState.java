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

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterMessage;
import org.neo4j.kernel.ha2.statemachine.State;

import java.net.URI;

/**
 * State machine for Paxos Proposer
 */
public enum ProposerState
    implements State<MultiPaxosContext, ProposerMessage>
{
    start
        {
            @Override
            public ProposerState handle( MultiPaxosContext context,
                                      Message<ProposerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case join:
                    {
                        if (context.clusterContext.isCoordinator())
                            return coordinator;
                        else
                            return proposer;
                    }

                    case propose:
                    {
                        // Joining cluster
                        ClusterMessage.ConfigurationChangeState state = message.getPayload();

                        context.clusterContext.getConfiguration().setNodes(state.getNodes());

                        propose(context, outgoing, state);
                        return coordinator;
                    }
                }

                return this;
            }
        },

    coordinator
        {
            @Override
            public ProposerState handle( MultiPaxosContext context,
                                      Message<ProposerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch(message.getMessageType())
                {
                    case propose:
                    {
                        Object payload = message.getPayload();
                        propose( context, outgoing, payload );
                        break;
                    }

                    case phase1Timeout:
                    {
                        InstanceId instanceId = message.getPayload();
                        ProposerInstance instance = context.proposerInstances.getProposerInstance( instanceId );
                        long ballot = instance.ballot + 100;
                        instance.phase1Timeout(ballot);
                        for( URI acceptor : context.getAcceptors() )
                        {
                            outgoing.process( message.copyHeadersTo(Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState( instanceId, ballot ) ), "instance"));
                        }
                        break;
                    }

                    case promise:
                    {
                        // P
                        ProposerMessage.PromiseState promiseState = message.getPayload();
                        ProposerInstance instance = context.proposerInstances.getProposerInstance( promiseState.getInstance() );

                        if (instance.state.equals(ProposerInstance.State.p1_pending) && instance.ballot == promiseState.getBallot())
                        {
                            instance.promise( promiseState );

                            if (instance.promises.size() == context.getMinimumQuorumSize())
                            {
                                context.timeouts.cancelTimeout( instance.id );

                                // No promises contained a value
                                if (instance.value_1 == null)
                                {
                                    // R0
                                    instance.ready(instance.value_2 == null ? context.bookedInstances.get( instance.id ) : instance.value_2, true);
                                } else
                                {
                                    // R1
                                    if (instance.value_2 == null)
                                    {
                                        // Another value was already associated with this instance. Push value back onto pending list
                                        context.pendingValues.offerFirst( context.bookedInstances.remove( instance.id ) );

                                        instance.ready( instance.value_1, false );
                                    } else if (instance.value_1.equals( instance.value_2 == null ? context.bookedInstances.get( instance.id ) : instance.value_2 ))
                                    {
                                        instance.ready( instance.value_2, instance.clientValue );
                                    } else if (instance.clientValue)
                                    {
                                        // Another value was already associated with this instance. Push value back onto pending list
                                        context.pendingValues.offerFirst( context.bookedInstances.remove( instance.id ) );

                                        instance.ready( instance.value_1, false );
                                    } else
                                    {
                                        // Another value was already associated with this instance. Push value back onto pending list
                                        context.pendingValues.offerFirst( context.bookedInstances.remove( instance.id ) );
                                        instance.ready( instance.value_1, false );
                                    }
                                }

                                // E: Send to Acceptors
                                instance.pending();
                                for( URI acceptor : context.getAcceptors() )
                                {
                                    outgoing.process( Message.to( AcceptorMessage.accept, acceptor, new AcceptorMessage.AcceptState( instance.id, instance.ballot, instance.value_2) ) );
                                }

                                context.timeouts.setTimeout( instance.id, Message.internal( ProposerMessage.phase2Timeout, instance.id ) );
                            }
                        }
                        break;
                    }

                    case phase2Timeout:
                    {
                        InstanceId instanceId = message.getPayload();
                        ProposerInstance instance = context.proposerInstances.getProposerInstance(instanceId);

                        if (instance.state.equals(ProposerInstance.State.p2_pending))
                        {
                            long ballot = instance.ballot + 100;
                            instance.phase2Timeout( ballot );

                            for( URI acceptor : context.getAcceptors() )
                            {
                                outgoing.process( message.copyHeadersTo(Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState(instanceId, ballot ) ), "instance"));
                            }

                            context.timeouts.setTimeout( instanceId, Message.internal( ProposerMessage.phase1Timeout, instanceId ) );
                        }
                    }

                    case reject:
                    {
                        // Denial of prepare

                        break;
                    }

                    case accepted:
                    {
                        ProposerMessage.AcceptedState acceptedState = message.getPayload();
                        ProposerInstance instance = context.proposerInstances.getProposerInstance( acceptedState.getInstance() );

                        // Sanity check the id
                        if (instance.state.equals(ProposerInstance.State.p2_pending))
                        {
                            instance.accepted(acceptedState);

                            if (instance.accepts.size() == context.getMinimumQuorumSize())
                            {
                                context.timeouts.cancelTimeout( instance.id );

                                instance.closed();

                                // Tell learners
                                for( URI learner : context.getLearners() )
                                {
                                    outgoing.process( Message.to( LearnerMessage.learn, learner, new LearnerMessage.LearnState( instance.id, instance.value_2 ) ));
                                }

                                context.bookedInstances.remove( instance.id );
                                instance.delivered();

                                // Check if we have anything pending - try to start process for it
                                if (!context.pendingValues.isEmpty() && context.bookedInstances.size() < 10)
                                {
                                    Object value = context.pendingValues.remove();
                                    System.out.println( "Restarting "+value +" booked:"+context.bookedInstances.size() );
                                    propose( context, outgoing, value );
                                }
                            }
                        }
                        break;
                    }

                    case leave:
                    {
                        return start;
                    }
                }
                
                return this;
            }

        },

    proposer
        {
            @Override
            public ProposerState handle( MultiPaxosContext paxosContext,
                                         Message<ProposerMessage> proposerMessageMessage,
                                         MessageProcessor outgoing
            )
                throws Throwable
            {
                // TODO Implement idle proposer state

                return this;
            }
        };

    private static void propose( MultiPaxosContext context, MessageProcessor outgoing, Object payload )
    {
        InstanceId instanceId = context.newInstanceId();

        context.bookedInstances.put( instanceId, payload );

        long ballot = 100 + context.getServerId(); // First server will have first ballot id be 101

        ProposerInstance instance = context.proposerInstances.getProposerInstance(instanceId);

        if (instance.state.equals(ProposerInstance.State.empty))
        {
            instance.propose(instanceId, ballot, context.clusterContext.configuration.getNodes());

            for( URI acceptor : context.getAcceptors() )
            {
                outgoing.process( Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState( instanceId, ballot ) ).setHeader( "instance", ""+instanceId ));
            }

            context.timeouts.setTimeout( instanceId, Message.internal( ProposerMessage.phase1Timeout, instanceId ) );
        } else
        {
            // Wait with this value - we have our hands full right now
            context.pendingValues.offerFirst( payload );
        }
    }
}
