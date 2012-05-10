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

import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

/**
 * State machine for Paxos Learner
 */
public enum LearnerState
    implements State<RingPaxosContext, LearnerMessage>
{
    start
        {
            @Override
            public LearnerState handle( RingPaxosContext context,
                                      Message<LearnerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case join:
                    {
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
            public LearnerState handle( RingPaxosContext context,
                                      Message<LearnerMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case learn:
                    {
                        LearnerMessage.LearnState learnState = ( LearnerMessage.LearnState) message.getPayload();
                        if (context.v_vid == learnState.getV_vid())
                        {
                            context.learnValue( );
                        } else
                        {
                            // TODO What does this mean? We're behind?
                        }
                        break;
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
