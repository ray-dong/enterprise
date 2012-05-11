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

import java.util.ArrayList;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

/**
 * State machine for Paxos Coordinator
 */
public enum CoordinatorState
    implements State<RingPaxosContext, CoordinatorMessage>
{
    start
        {
            @Override
            public CoordinatorState handle( RingPaxosContext context,
                                      Message<CoordinatorMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case join:
                    {
                        return coordinator;
                    }
                }

                return this;
            }
        },

    coordinator
        {
            @Override
            public CoordinatorState handle( RingPaxosContext context,
                                      Message<CoordinatorMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch(message.getMessageType())
                {
                    case propose:
                    {
                        // Task 1
                        context.c_rnd = context.c_rnd + 1;
                        context.ring = new ArrayList<String>( context.getAcceptors() );
                        context.ring.remove( context.coordinator );
                        context.ring.add( context.coordinator ); // Make sure coordinator is last

                        context.v = message.getPayload();
                        for( String acceptor : context.ring )
                        {
                            if (!acceptor.equals( context.getMe() ))
                                outgoing.process( Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState(context.c_rnd,context.ring ) ));
                        }

                        // Coordinator automatically promises
                        context.rnd = context.c_rnd;

                        context.promiseStates.add( new CoordinatorMessage.PromiseState( context.c_rnd, context.v_rnd, context.v_val ) );
                        break;
                    }

                    case promise:
                    {
                        // Task 3
                        CoordinatorMessage.PromiseState promiseState = (CoordinatorMessage.PromiseState) message.getPayload();
                        if (context.c_rnd == promiseState.getRnd())
                        {
                            context.promiseStates.add( promiseState );

                            if (context.promiseStates.size() == context.getMinimumQuorumSize())
                            {
                                // Enough acceptors (including coordinator itself) have promised now
                                long k = 0;
                                Object v_val = null;
                                for( CoordinatorMessage.PromiseState state : context.promiseStates )
                                {
                                    if (state.getV_rnd() > k)
                                    {
                                        k = state.getV_rnd();
                                        v_val = state.getV_val();
                                    }
                                }

                                if (k == 0)
                                {
                                    context.c_val = context.v;
                                } else
                                {
                                    context.c_val = v_val;
                                }

                                context.c_vid = context.c_val.hashCode();

                                for( String acceptor : context.getAcceptors() )
                                {
                                    outgoing.process( Message.to( AcceptorMessage.accept, acceptor, new AcceptorMessage.AcceptState( context.c_rnd, context.c_val, context.c_vid ) ) );
                                }

                                for( String learner : context.getLearners() )
                                {
                                    if (!context.getAcceptors().contains( learner ))
                                        outgoing.process( Message.to( AcceptorMessage.accept, learner, new AcceptorMessage.AcceptState( context.c_rnd, context.c_val, context.c_vid ) ) );
                                }
                            }
                        }
                        break;
                    }

                    case denial:
                    {
                        // Denial of prepare

                        break;
                    }

                    case accepted:
                    {
                        // Task 5 -- Coordinator version
                        CoordinatorMessage.AcceptedState acceptedState = (CoordinatorMessage.AcceptedState) message.getPayload();
                        if (context.v_vid == acceptedState.getC_vid())
                        {
                            // Tell learners
                            for( String learner : context.getLearners() )
                            {
                                outgoing.process( Message.to( LearnerMessage.learn, learner, new LearnerMessage.LearnState( acceptedState.getC_vid() ) ));
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
}
