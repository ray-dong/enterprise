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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageSource;
import org.neo4j.com_2.message.MessageType;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;
import org.neo4j.kernel.ha2.timeout.TimeoutStrategy;
import org.neo4j.kernel.ha2.timeout.Timeouts;

import static org.neo4j.com_2.message.Message.*;

/**
 * TODO
 */
public class ConnectedStateMachines
    implements MessageProcessor, MessageSource
{
    private final Logger logger = Logger.getLogger( ConnectedStateMachines.class.getName() );
    
    private final MessageProcessor sender;
    private Timeouts timeouts;
    private final Map<Class<? extends MessageType>,StateMachine> stateMachines = new LinkedHashMap<Class<? extends MessageType>, StateMachine>(  );

    private final List<MessageProcessor> outgoingProcessors = new ArrayList<MessageProcessor>(  );
    private final OutgoingMessageProcessor outgoing;

    public ConnectedStateMachines( MessageSource source,
                                   final MessageProcessor sender,
                                   TimeoutStrategy timeoutStrategy
    )
    {
        this.sender = sender;
        this.timeouts = new Timeouts( this, timeoutStrategy );

        outgoing = new OutgoingMessageProcessor();
        source.addMessageProcessor( this );
    }
    
    public Timeouts getTimeouts()
    {
        return timeouts;
    }

    public synchronized void addStateMachine(StateMachine stateMachine)
    {
        stateMachines.put(stateMachine.getMessageType(), stateMachine);
    }

    public synchronized void removeStateMachine(StateMachine stateMachine)
    {
        stateMachines.remove(stateMachine.getMessageType());
    }

    public Iterable<StateMachine> getStateMachines()
    {
        return stateMachines.values();
    }

    @Override
    public void addMessageProcessor( MessageProcessor messageProcessor )
    {
        outgoingProcessors.add( messageProcessor );
    }

    public OutgoingMessageProcessor getOutgoing()
    {
        return outgoing;
    }

    @Override
    public synchronized void process( Message<? extends MessageType> message )
    {
        // Lock timeouts while we are processing the message
        synchronized( timeouts )
        {
            StateMachine stateMachine = stateMachines.get( message.getMessageType().getClass() );
            if (stateMachine == null)
                return; // No StateMachine registered for this MessageType type - Ignore this

            stateMachine.handle( message, outgoing );

            // Process and send messages
            // Allow state machines to send messages to each other as well in this loop
            Message<? extends MessageType> outgoingMessage;
            while ((outgoingMessage = outgoing.nextOutgoingMessage()) != null)
            {
                message.copyHeadersTo( outgoingMessage, CONVERSATION_ID, CREATED_BY );

                for( MessageProcessor outgoingProcessor : outgoingProcessors )
                {
                    try
                    {
                        outgoingProcessor.process( outgoingMessage );
                    }
                    catch( Throwable e )
                    {
                        logger.warning( "Outgoing message processor threw exception" );
                        logger.throwing( ConnectedStateMachines.class.getName(), "process", e );
                    }
                }

                if( outgoingMessage.hasHeader( Message.TO ))
                {
                    try
                    {
                        sender.process( outgoingMessage );
                    }
                    catch( Throwable e )
                    {
                        logger.warning( "Message sending threw exception" );
                        logger.throwing( ConnectedStateMachines.class.getName(), "process", e );
                    }
                } else
                {
                    // Deliver internally if possible
                    StateMachine internalStatemachine = stateMachines.get( outgoingMessage.getMessageType().getClass() );
    //                if (internalStatemachine != null && stateMachine != internalStatemachine )
                    if (internalStatemachine != null)
                    {
                        internalStatemachine.handle( (Message)outgoingMessage, outgoing );
                    }
                }
            }
        }
    }

    public void addStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        for( StateMachine stateMachine : stateMachines.values() )
        {
            stateMachine.addStateTransitionListener( stateTransitionListener );
        }
    }

    public void removeStateTransitionListener(StateTransitionListener stateTransitionListener)
    {
        for( StateMachine stateMachine : stateMachines.values() )
        {
            stateMachine.removeStateTransitionListener( stateTransitionListener );
        }
    }

    @Override
    public String toString()
    {
        List<String> states = new ArrayList<String>();
        for( StateMachine stateMachine : stateMachines.values() )
        {
            states.add( stateMachine.getState().getClass().getSuperclass().getSimpleName()+":"+stateMachine.getState().toString() );
        }
        return states.toString();
    }

    private class OutgoingMessageProcessor
        implements MessageProcessor
    {
        private Queue<Message<? extends MessageType>> outgoingMessages = new LinkedList<Message<? extends MessageType>>();

        @Override
        public void process( Message<? extends MessageType> message )
        {
            outgoingMessages.offer( message );
        }

        public Message<? extends MessageType> nextOutgoingMessage()
        {
            return outgoingMessages.poll();
        }
    }
}
