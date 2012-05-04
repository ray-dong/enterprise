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

import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

/**
 * State machine for Paxos Proposer
 */
public enum ProposerState
    implements State<PaxosContext, ProposerMessage, ProposerState>
{
    start
        {
            @Override
            public ProposerState handle( PaxosContext context,
                                      Message<ProposerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case join:
                    {
                        context.proposerInstances.clear();
                        for (int i = 0; i < 100; i++)
                            context.proposerInstances.add( new ProposerInstance() );

                        if (context.clusterConfiguration.getCoordinator().equals( context.getMe() ))
                            return coordinator;
                        else
                            return proposer;
                    }
                }

                return this;
            }
        },

    coordinator
        {
            @Override
            public ProposerState handle( PaxosContext context,
                                      Message<ProposerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch(message.getMessageType())
                {
                    case propose:
                    {
                        context.pendingValues.push( message.getPayload() );

                        long instanceId = context.lastInstanceId++;
                        long ballot = 100 + context.getServerId(); // First server will have first ballot id be 101

                        ProposerInstance instance = context.proposerInstances.get( context.getProposerInstanceIndex(instanceId) );
                        instance.propose(instanceId, ballot);

                        for( String acceptor : context.clusterConfiguration.getAcceptors() )
                        {
                            outgoing.process( Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState(instanceId, ballot ) ).setHeader( "instance", ""+instanceId ));
                        }

                        break;
                    }

                    case phase1Timeout:
                    {
                        long instanceId = Long.parseLong( message.getHeader( "instance" ) );
                        ProposerInstance instance = context.proposerInstances.get( context.getProposerInstanceIndex(instanceId) );
                        long ballot = instance.ballot + 100;
                        instance.phase1Timeout(ballot);
                        for( String acceptor : context.clusterConfiguration.getAcceptors() )
                        {
                            outgoing.process( message.copyHeadersTo(Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState( instanceId, ballot ) ), "instance"));
                        }
                        break;
                    }

                    case promise:
                    {
                        // P
                        ProposerMessage.PromiseState promiseState = (ProposerMessage.PromiseState) message.getPayload();
                        ProposerInstance instance = context.proposerInstances.get( context.getProposerInstanceIndex(promiseState.getInstance()) );

                        if (instance.ballot == promiseState.getBallot())
                        {
                            instance.promise( promiseState );

                            if (instance.promises.size() == context.getMinimumQuorumSize())
                            {
                                // No promises contained a value
                                if (instance.value_1 == null)
                                {
                                    // R0
                                    instance.ready(instance.value_2 == null ? context.pendingValues.poll() : instance.value_2, true);
                                } else
                                {
                                    // R1
                                    if (instance.value_2 == null)
                                    {
                                        instance.ready( instance.value_1, false );
                                    } else if (instance.value_1.equals( instance.value_2 == null ? context.pendingValues.poll() : instance.value_2 ))
                                    {
                                        instance.ready( instance.value_2, instance.clientValue );
                                    } else if (instance.clientValue)
                                    {
                                        instance.ready( instance.value_1, false );
                                    } else
                                    {
                                        // TODO Paxos Made Code says to discard client value, but we never popped it from pending list
                                        instance.ready( instance.value_1, false );
                                    }
                                }

                                // E: Send to Acceptors
                                instance.pending();
                                for( String acceptor : context.getAcceptors() )
                                {
                                    outgoing.process( Message.to( AcceptorMessage.accept, acceptor, new AcceptorMessage.AcceptState( instance.id, instance.ballot, instance.value_2) ) );
                                }
                            }
                        }
                        break;
                    }

                    case phase2Timeout:
                    {
                        long instanceId = Long.parseLong( message.getHeader( "instance" ) );
                        ProposerInstance instance = context.proposerInstances.get( context.getProposerInstanceIndex(instanceId) );
                        long ballot = instance.ballot + 100;
                        instance.phase2Timeout( ballot );

                        outgoing.process( message );

                        for( String acceptor : context.clusterConfiguration.getAcceptors() )
                        {
                            outgoing.process( message.copyHeadersTo(Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState(instanceId, ballot ) ), "instance"));
                        }
                    }

                    case reject:
                    {
                        // Denial of prepare

                        break;
                    }

                    case accepted:
                    {
                        ProposerMessage.AcceptedState acceptedState = (ProposerMessage.AcceptedState) message.getPayload();
                        ProposerInstance instance = context.proposerInstances.get( context.getProposerInstanceIndex(acceptedState.getInstance()) );

                        // Sanity check the id
                        if (instance.id == acceptedState.getInstance())
                        {
                            instance.accepted(acceptedState);

                            if (instance.accepts.size() == context.getMinimumQuorumSize())
                            {
                                instance.closed();

                                // Tell learners
                                for( String learner : context.getLearners() )
                                {
                                    outgoing.process( Message.to( LearnerMessage.learn, learner, new LearnerMessage.LearnState( instance.id, instance.value_2 ) ));
                                }

                                instance.delivered();
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
            public ProposerState handle( PaxosContext paxosContext,
                                         Message<ProposerMessage> proposerMessageMessage,
                                         MessageProcessor outgoing
            )
                throws Throwable
            {
                // TODO Implement idle proposer state

                return this;
            }
        }
}
