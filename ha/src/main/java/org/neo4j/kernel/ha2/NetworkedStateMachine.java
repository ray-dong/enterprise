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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.neo4j.com2.MessageReceiver;
import org.neo4j.com2.MessageSender;
import org.neo4j.com2.NetworkMessageListener;
import org.neo4j.kernel.LifecycleAdapter;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.statemachine.message.Message;
import org.neo4j.kernel.ha2.statemachine.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.message.MessageType;

import static org.neo4j.kernel.ha2.statemachine.message.Message.*;

/**
 * TODO
 */
public class NetworkedStateMachine
    extends LifecycleAdapter
    implements MessageProcessor
{
    private Logger logger = Logger.getLogger( NetworkedStateMachine.class.getName() );
    
    private Map<String, ExpectationFailure> expectations;

    private ScheduledExecutorService expectationScheduler;
    private RingParticipant me;
    private final MessageReceiver receiver;
    private MessageSender sender;
    private StateMachine stateMachine;

    private List<Message> outgoingMessages = new ArrayList<Message>();
    private List<MessageProcessor> outgoingProcessors = new ArrayList<MessageProcessor>(  );
    protected final MessageProcessor outgoing;
    protected final NetworkMessageListener messageReceiver;

    public NetworkedStateMachine( MessageReceiver receiver,
                                  final MessageSender sender,
                                  final StateMachine stateMachine
    )
    {
        this.receiver = receiver;
        this.sender = sender;
        this.stateMachine = stateMachine;
        expectations = new ConcurrentHashMap<String, ExpectationFailure>();

        outgoing = new OutgoingMessageProcessor();
        messageReceiver = new StateMachineMessageListenerAdapter();
    }

    @Override
    public void init()
        throws Throwable
    {
        expectationScheduler = Executors.newScheduledThreadPool( 3 );

        receiver.addMessageListener( messageReceiver );
    }

    @Override
    public void start()
        throws Throwable
    {
        logger.info( "==== " + me + " starts" );
    }

    @Override
    public void stop()
        throws Throwable
    {
    }

    @Override
    public void shutdown()
        throws Throwable
    {
        expectationScheduler.shutdown();
    }
    
    public void setMe(RingParticipant participant)
    {
        this.me = participant;
    }
    
    public void addOutgoingProcessor( MessageProcessor messageProcessor )
    {
        outgoingProcessors.add( messageProcessor );
    }

    @Override
    public synchronized void process( Message message )
    {
        // Cancel existing timeout
        {
            ExpectationFailure expectationFailure = expectations.remove( message.getHeader( CONVERSATION_ID ) );
            if( expectationFailure != null )
            {
                expectationFailure.cancel();
            }
        }

        stateMachine.receive( message, outgoing );

        // Process and send messages
        for( Message outgoingMessage : outgoingMessages )
        {
            message.copyHeadersTo( outgoingMessage, CONVERSATION_ID, CREATED_BY );

            for( MessageProcessor outgoingProcessor : outgoingProcessors )
            {
                outgoingProcessor.process( outgoingMessage );
            }

            if( outgoingMessage.hasHeader( Message.TO ) )
            {
                outgoingMessage.setHeader( Message.FROM, me.getServerId() );

                String to = outgoingMessage.getHeader( Message.TO );
                sender.send( to, outgoingMessage );

                // Create timeout
                MessageType messageType = outgoingMessage.getMessageType();
                if( messageType.failureMessage() != null )
                {
                    String conversationId = outgoingMessage.getHeader( CONVERSATION_ID );
                    ExpectationFailure expectationFailure = new ExpectationFailure( conversationId, messageType
                        .failureMessage() );
                    expectationScheduler.schedule( expectationFailure, 3, TimeUnit.SECONDS );
                    expectations.put( conversationId, expectationFailure );
                }
            }
        }
        outgoingMessages.clear();
    }

    class ExpectationFailure
        implements Runnable
    {
        private String conversationId;
        private MessageType messageType;
        private boolean cancelled = false;

        public ExpectationFailure( String conversationId, MessageType messageType )
        {
            this.conversationId = conversationId;
            this.messageType = messageType;
        }

        public synchronized void cancel()
        {
            cancelled = true;
        }

        @Override
        public synchronized void run()
        {
            if( !cancelled )
            {
                messageReceiver.received( Message.internal( messageType, "Timed out" ).setHeader( CONVERSATION_ID, conversationId ) );
            }
        }
    }

    private class OutgoingMessageProcessor
        implements MessageProcessor
    {
        @Override
        public void process( Message message )
        {
            outgoingMessages.add( message );
        }
    }

    private class StateMachineMessageListenerAdapter
        implements NetworkMessageListener
    {
        @Override
        public void received( Object message )
        {
            Message stateMessage = (Message) message;

            process( stateMessage );
        }
    }
}
