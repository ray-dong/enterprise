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
 * State machine for Paxos Acceptor
 */
public enum AcceptorState
    implements State<MultiPaxosContext, AcceptorMessage>
{
    start
        {
            @Override
            public AcceptorState handle( MultiPaxosContext context,
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
            public AcceptorState handle( MultiPaxosContext context,
                                      Message<AcceptorMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case prepare:
                    {
                        AcceptorMessage.PrepareState prepareState = message.getPayload();
                        PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( prepareState.getInstanceId() );

                        if ( prepareState.getBallot() >= instance.ballot)
                        {
                            instance.prepare(prepareState.getInstanceId(), prepareState.getBallot());

                            outgoing.process(Message.respond( ProposerMessage.promise, message, new ProposerMessage.PromiseState( instance.id, instance.ballot, instance.value_2 ) ));
                        } else
                        {
                            // Optimization - explicit reject
                            LoggerFactory.getLogger(AcceptorState.class).info( "Reject "+prepareState.getInstanceId()+" ballot:"+instance.ballot );
                            outgoing.process(Message.respond( ProposerMessage.rejectPropose, message, new ProposerMessage.RejectProposeState( instance.id, instance.ballot ) ));
                        }
                        break;
                    }

                    case accept:
                    {
                        // Task 4
                        AcceptorMessage.AcceptState acceptState = message.getPayload();
                        PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( acceptState.getInstance() );

                        if (acceptState.getInstance() != null)
                        {
                            if (acceptState.getBallot() == instance.ballot)
                            {
                                instance.accept(acceptState.getValue());

                                outgoing.process( Message.respond( ProposerMessage.accepted, message, new ProposerMessage.AcceptedState( instance.id ) ) );
                            } else
                            {
                                outgoing.process(Message.respond( ProposerMessage.rejectAccept, message, new ProposerMessage.RejectAcceptState( instance.id ) ));
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
