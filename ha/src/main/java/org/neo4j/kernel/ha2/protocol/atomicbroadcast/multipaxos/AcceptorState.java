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
    implements State<AcceptorContext, AcceptorMessage>
{
    start
        {
            @Override
            public AcceptorState handle( AcceptorContext context,
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
            public AcceptorState handle( AcceptorContext context,
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
                        AcceptorInstance instance = context.getAcceptorInstance( prepareState.getInstanceId() );

                        if ( prepareState.getBallot() >= instance.getBallot())
                        {
                            context.promise(instance, prepareState.getBallot() );

                            outgoing.process(Message.respond( ProposerMessage.promise, message, new ProposerMessage.PromiseState( prepareState.getInstanceId(), prepareState.getBallot(), instance.getValue() ) ));
                        } else
                        {
                            // Optimization - explicit reject
                            LoggerFactory.getLogger(AcceptorState.class).info( "Reject "+prepareState.getInstanceId()+" ballot:"+instance.getBallot() );
                            outgoing.process(Message.respond( ProposerMessage.rejectPropose, message, new ProposerMessage.RejectProposeState( prepareState.getInstanceId(), instance.getBallot() ) ));
                        }
                        break;
                    }

                    case accept:
                    {
                        // Task 4
                        AcceptorMessage.AcceptState acceptState = message.getPayload();
                        AcceptorInstance instance = context.getAcceptorInstance( acceptState.getInstanceId() );

                        if (acceptState.getInstanceId() != null)
                        {
                            if (acceptState.getBallot() == instance.getBallot())
                            {
                                context.accept(instance, acceptState.getValue());
                                instance.accept(acceptState.getValue());

                                outgoing.process( Message.respond( ProposerMessage.accepted, message, new ProposerMessage.AcceptedState( acceptState.getInstanceId() ) ) );
                            } else
                            {
                                LoggerFactory.getLogger(AcceptorState.class).info( "Reject "+acceptState.getInstanceId()+" accept ballot:"+acceptState.getBallot()+" actual ballot:"+instance.getBallot() );
                                outgoing.process(Message.respond( ProposerMessage.rejectAccept, message, new ProposerMessage.RejectAcceptState( acceptState.getInstanceId() ) ));
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
