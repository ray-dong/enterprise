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
 * State machine for Paxos Acceptor
 */
public enum AcceptorState
    implements State<RingPaxosContext, AcceptorMessage>
{
    start
        {
            @Override
            public AcceptorState handle( RingPaxosContext context,
                                      Message<AcceptorMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case join:
                    {
                        return acceptor;
                    }
                }

                return this;
            }
        },

    acceptor
        {
            @Override
            public AcceptorState handle( RingPaxosContext context,
                                      Message<AcceptorMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case prepare:
                    {
                        // Task 2
                        AcceptorMessage.PrepareState prepareState = (AcceptorMessage.PrepareState) message.getPayload();
                        if ( prepareState.getC_rnd() > context.rnd)
                        {
                            context.rnd = prepareState.getC_rnd();
                            context.ring = prepareState.getC_ring();
                            outgoing.process(Message.to( CoordinatorMessage.promise, message.getHeader( Message.FROM ), new CoordinatorMessage.PromiseState( context.rnd, context.v_rnd, context.v_val ) ));
                        } else
                        {
                            // Optimization - explicit denial
                            outgoing.process(Message.to( CoordinatorMessage.denial, message.getHeader( Message.FROM ), new CoordinatorMessage.DenialState( prepareState.getC_rnd() )));
                        }
                        break;
                    }

                    case accept:
                    {
                        // Task 4
                        AcceptorMessage.AcceptState acceptState = ( AcceptorMessage.AcceptState) message.getPayload();
                        if (acceptState.getC_rnd() >= context.rnd)
                        {
                            context.v_rnd = acceptState.getC_rnd();
                            context.v_val = acceptState.getC_val();
                            context.v_vid = acceptState.getC_vid();

                            if (context.isFirst())
                                outgoing.process( Message.to( AcceptorMessage.accepted, context.getSuccessor(), new AcceptorMessage.AcceptedState(acceptState.getC_rnd(), acceptState.getC_vid()) ) );
                        } else
                        {
                            // TODO How to deal with this? What does this case mean?
                        }
                        break;
                    }

                    case accepted:
                    {
                        // Task 5 - Acceptor version
                        AcceptorMessage.AcceptedState acceptedState = (AcceptorMessage.AcceptedState) message.getPayload();
                        if (context.v_vid == acceptedState.getC_vid())
                        {
                            if (context.isLastAcceptor())
                            {
                                // Tell Coordinator
                                outgoing.process( Message.to( CoordinatorMessage.accepted, context.getSuccessor(), new CoordinatorMessage.AcceptedState(acceptedState.getV_rnd(), acceptedState.getC_vid()) ) );
                            } else
                            {
                                // Tell next Acceptor about this decision
                                outgoing.process( Message.to( AcceptorMessage.accepted, context.getSuccessor(), acceptedState ) );
                            }
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
        },
}
