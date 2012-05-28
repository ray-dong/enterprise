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
import org.neo4j.kernel.ha2.statemachine.State;
import org.slf4j.LoggerFactory;

/**
 * State machine for Paxos Learner
 */
public enum LearnerState
    implements State<MultiPaxosContext, LearnerMessage>
{
    start
        {
            @Override
            public LearnerState handle( MultiPaxosContext context,
                                      Message<LearnerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case join:
                    {
                        return learner;
                    }
                }

                return this;
            }
        },

    learner
        {
            @Override
            public LearnerState handle( MultiPaxosContext context,
                                      Message<LearnerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case learn:
                    {
                        LearnerMessage.LearnState learnState = message.getPayload();
                        PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( learnState.getInstanceId() );

                        // Skip if we already know about this
                        if( learnState.getInstanceId().getId() <= context.learnerContext.lastDeliveredInstanceId )
                        {
                            break;
                        }

                        context.learnerContext.lastLearnedInstanceId = Math.max( context.learnerContext.lastLearnedInstanceId, learnState.getInstanceId().getId());

                        instance.closed(learnState.getValue());

                        // If this is the next instance to be learned, then do so and check if we have anything pending to be learnt
                        if (learnState.getInstanceId().equals( new InstanceId( context.learnerContext.lastDeliveredInstanceId +1) ))
                        {
                            instance.delivered();
                            outgoing.process(Message.internal(AtomicBroadcastMessage.receive, learnState.getValue()));
                            context.learnerContext.lastDeliveredInstanceId = learnState.getInstanceId().getId();

                            long instanceId = learnState.getInstanceId().getId()+1;
                            while ((instance = context.getPaxosInstances().getPaxosInstance( new InstanceId( instanceId ) )).isState( PaxosInstance.State.closed ))
                            {
                                instance.delivered();
                                outgoing.process(Message.internal(AtomicBroadcastMessage.receive, instance.value_2));
                                context.learnerContext.lastDeliveredInstanceId = instance.id.getId();

                                instanceId++;
                            }

                            if (instanceId == context.learnerContext.lastLearnedInstanceId+1)
                            {
                                // No hole - all is ok
                                // Cancel potential timeout, if one is active
                                context.timeouts.cancelTimeout( "learn" );
                            } else
                            {
                                // Found hole - we're waiting for this to be filled, i.e. timeout already set
                                LoggerFactory.getLogger( LearnerState.class ).warn( "*** HOLE! WAITING FOR "+instanceId);

                            }
                        } else
                        {
                            // Set timeout waiting for values to come in
                            context.timeouts.cancelTimeout( "learn" );
                            context.timeouts.setTimeout( "learn", Message.timeout( LearnerMessage.learnTimedout, message ) );
                        }
                        break;
                    }

                    case learnTimedout:
                    {
                        // Timed out waiting for learned values - send explicit request to someone
                        if (context.learnerContext.lastDeliveredInstanceId != context.learnerContext.lastLearnedInstanceId)
                        {

                            for (long instanceId = context.learnerContext.lastDeliveredInstanceId +1; instanceId < context.learnerContext.lastLearnedInstanceId; instanceId++)
                            {
                                InstanceId id = new InstanceId( instanceId );
                                PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( id );
                                if (!instance.isState( PaxosInstance.State.closed ) && !instance.isState( PaxosInstance.State.delivered ))
                                {
                                    outgoing.process( Message.to( LearnerMessage.learnRequest, context.clusterContext.getConfiguration().getNodes().get( 0 ).toString(), new LearnerMessage.LearnRequestState(id) ) );
                                }
                            }

                            // Set another timeout
                            context.timeouts.setTimeout( "learn", Message.timeout( LearnerMessage.learnTimedout, message ) );
                        }
                        break;
                    }

                    case learnRequest:
                    {
                        // Someone wants to learn a value that we might have
                        LearnerMessage.LearnRequestState state = message.getPayload();
                        PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( state.getInstanceId() );
                        if (instance.isState( PaxosInstance.State.closed ) || instance.isState( PaxosInstance.State.delivered ))
                        {
                            outgoing.process( Message.respond( LearnerMessage.learn, message, new LearnerMessage.LearnState( instance.id, instance.value_2 ) ));
                        } else
                        {
                            LoggerFactory.getLogger(getClass()).debug( "Did not have learned value for instance "+state.getInstanceId() );
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
        }
}
