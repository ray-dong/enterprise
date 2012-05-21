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
                        // Initialize all learner instances
                        context.learnerContext.learnerInstances.clear();
                        for (int i = 0; i < 10; i++)
                            context.learnerContext.learnerInstances.add( new LearnerInstance() );

                        // TODO Do formal join process
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
                        if (learnState.getInstanceId().equals( new InstanceId( context.learnerContext.lastReceivedInstanceId +1) ))
                        {
                            outgoing.process(Message.internal(AtomicBroadcastMessage.receive, learnState.getValue()));

                            context.learnerContext.lastReceivedInstanceId = learnState.getInstanceId().getId();
                            for (int i=1; i < context.learnerContext.learnerInstances.size(); i++)
                            {
                                int index = context.learnerContext.getLearnerInstanceIndex(learnState.getInstanceId().getId()+i);
                                LearnerInstance learnerInstance = context.learnerContext.learnerInstances.get( index );
                                if ( learnerInstance.instanceId != null)
                                {
                                    outgoing.process(Message.internal(AtomicBroadcastMessage.receive, learnerInstance.value));
                                    context.learnerContext.lastReceivedInstanceId = learnerInstance.instanceId.getId();
                                    learnerInstance.instanceId = null;
                                    learnerInstance.value = null;
                                } else
                                {
                                    // Found hole - wait for it to be filled!
                                    return this;
                                }
                            }
                        }
                        else if (learnState.getInstanceId().getId()<= context.learnerContext.lastReceivedInstanceId)
                        {
                            // We have already learned this - ignore
                        } else
                        {
                            // Store it and wait for hole to be filled
                            int distance = (int)(learnState.getInstanceId().getId() - context.learnerContext.lastReceivedInstanceId);
                            if (distance < context.learnerContext.learnerInstances.size()-1)
                            {
                                int index = context.learnerContext.getLearnerInstanceIndex(learnState.getInstanceId().getId());
                                context.learnerContext.learnerInstances.get( index ).set(learnState);
                            } else
                            {
                                // TODO Value has been discarded because there is no space in the index. Have to refetch it later from someone else
                            }
                        }
                        break;
                    }

                    case learnRequest:
                    {
                        LearnerMessage.LearnRequestState state = message.getPayload();

                    }

                    case leave:
                    {
                        // TODO Do formal leave process
                        return start;
                    }
                }
                
                return this;
            }
        }
}
