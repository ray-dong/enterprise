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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.com2.message.MessageType;
import org.neo4j.kernel.ha2.statemachine.State;
import org.neo4j.kernel.ha2.statemachine.StateMachine;

import static org.neo4j.com2.message.Message.*;

/**
 * TODO
 */
public class ConnectedStateMachines
    implements MessageProcessor, MessageSource
{
    private final Logger logger = Logger.getLogger( ConnectedStateMachines.class.getName() );
    
    private final MessageProcessor sender;
    private final Map<Class<? extends Enum<?>>,StateMachine<?,?,?>> stateMachines = new HashMap<Class<? extends Enum<?>>, StateMachine<?,?,?>>(  );

    private final List<MessageProcessor> outgoingProcessors = new ArrayList<MessageProcessor>(  );
    private final OutgoingMessageProcessor outgoing;

    public ConnectedStateMachines( MessageSource source,
                                   final MessageProcessor sender
    )
    {
        this.sender = sender;

        outgoing = new OutgoingMessageProcessor();
        source.addMessageProcessor( this );
    }
    
    public synchronized <CONTEXT, MESSAGETYPE extends Enum<MESSAGETYPE> & MessageType, STATE extends State<CONTEXT, MESSAGETYPE, STATE>> void addStateMachine(StateMachine<CONTEXT, MESSAGETYPE,STATE> stateMachine)
    {
        stateMachines.put(stateMachine.getMessageType(), stateMachine);
    }

    public synchronized <CONTEXT, MESSAGETYPE extends Enum<MESSAGETYPE> & MessageType, STATE extends State<CONTEXT, MESSAGETYPE, STATE>> void removeStateMachine(StateMachine<CONTEXT, MESSAGETYPE,STATE> stateMachine)
    {
        stateMachines.remove(stateMachine.getMessageType());
    }

    @Override
    public void addMessageProcessor( MessageProcessor messageProcessor )
    {
        outgoingProcessors.add( messageProcessor );
    }

    @Override
    public synchronized <MESSAGETYPE extends Enum<MESSAGETYPE> & MessageType> void process( Message<MESSAGETYPE> message )
    {
        StateMachine<?,MESSAGETYPE,?> stateMachine = (StateMachine<?, MESSAGETYPE, ?>) stateMachines.get( message.getMessageType().getClass() );
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
                StateMachine<?,?,?> internalStatemachine = stateMachines.get( outgoingMessage.getMessageType().getClass() );
                if (internalStatemachine != null && stateMachine != internalStatemachine )
                {
                    internalStatemachine.handle( (Message)outgoingMessage, outgoing );
                }
            }
        }
    }

    private class OutgoingMessageProcessor
        implements MessageProcessor
    {
        @Override
        public <MESSAGETYPE extends Enum<MESSAGETYPE> & MessageType> void process( Message<MESSAGETYPE> message )
        {
            outgoingMessages.offer( message );
        }

        private Queue<Message<? extends MessageType>> outgoingMessages = new LinkedList<Message<? extends MessageType>>();

        public Message<? extends MessageType> nextOutgoingMessage()
        {
            return outgoingMessages.poll();
        }
    }
}
