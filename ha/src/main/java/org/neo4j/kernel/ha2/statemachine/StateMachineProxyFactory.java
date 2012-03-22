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

package org.neo4j.kernel.ha2.statemachine;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageType;

/**
 * TODO
 */
public class StateMachineProxyFactory<MESSAGETYPE extends Enum<MESSAGETYPE> & MessageType>
    implements MessageProcessor
{
    private StateMachine<?,MESSAGETYPE,?> stateMachine;
    private MessageProcessor incoming;
    private StateMachineConversations conversations;
    private String serverId;
    private Class<MESSAGETYPE> messageTypeEnum;

    private Map<String, ResponseFuture> responseFutureMap = new ConcurrentHashMap<String, ResponseFuture>(  );
    
    
    public StateMachineProxyFactory(String serverId, Class<MESSAGETYPE> messageTypeEnum, StateMachine<?,MESSAGETYPE,?> stateMachine, MessageProcessor incoming, StateMachineConversations conversations )
    {
        this.serverId = serverId;
        this.messageTypeEnum = messageTypeEnum;
        this.stateMachine = stateMachine;
        this.incoming = incoming;
        this.conversations = conversations;
    }
    
    public <CLIENT> CLIENT newProxy(Class<CLIENT> proxyInterface)
    {
        stateMachine.checkValidProxyInterface( proxyInterface );

        return proxyInterface.cast( Proxy.newProxyInstance( proxyInterface.getClassLoader(), new Class<?>[]{ proxyInterface }, new StateMachineProxyHandler( this ) ) );
    }

    public void addStateTransitionListener( StateTransitionListener<MESSAGETYPE> stateTransitionListener )
    {
        stateMachine.addStateTransitionListener( stateTransitionListener );
    }

    Object invoke( Method method, Object arg )
        throws Throwable
    {
        String conversationId = conversations.getNextConversationId();

        MESSAGETYPE typeAsEnum = Enum.valueOf( messageTypeEnum, method.getName() );
        Message<MESSAGETYPE> message = Message.internal( typeAsEnum, arg ).setHeader( Message.CONVERSATION_ID, conversationId ).setHeader( Message.CREATED_BY, serverId );

        if (method.getReturnType().equals( Void.TYPE ))
        {
            incoming.process( message );
            return null;
        }
        else
        {
            ResponseFuture future = new ResponseFuture( typeAsEnum );
            responseFutureMap.put( conversationId, future );
            incoming.process( message );

            return future;
        }
    }

    @Override
    public void process( Message message )
    {
        if (!responseFutureMap.isEmpty())
        {
            if (message.hasHeader( Message.TO ))
            {
                String conversationId = message.getHeader( Message.CONVERSATION_ID );
                ResponseFuture future = responseFutureMap.get( conversationId );
                if (future != null && !future.wasInitiatedBy(message.getMessageType().name()))
                {
                    future.updateMessage( message );
                }

            } else
            {
                String conversationId = message.getHeader( Message.CONVERSATION_ID );
                ResponseFuture future = responseFutureMap.get( conversationId );
                if (future != null && !future.wasInitiatedBy(message.getMessageType().name()))
                {
                    future.setResponse( message );
                    responseFutureMap.remove( conversationId );
                }
            }
        }
    }

    class ResponseFuture
        implements Future<Object>
    {
        private MessageType initiatedByMessageType;

        private Message response;

        ResponseFuture( MessageType initiatedByMessageType)
        {
            this.initiatedByMessageType = initiatedByMessageType;
        }

        public boolean wasInitiatedBy( String name )
        {
            return initiatedByMessageType.name().equals( name );
        }

        public synchronized void updateMessage( Message message )
        {
            initiatedByMessageType = MessageType.class.cast(message.getMessageType());
        }

        public synchronized void setResponse( Message response )
        {
            this.response = response;
            this.notifyAll();
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return response != null;
        }

        @Override
        public synchronized Object get()
            throws InterruptedException, ExecutionException
        {
            if (response != null)
            {
                return getResult();
            }

            this.wait();
            
            return getResult();
        }

        private Object getResult()
            throws InterruptedException, ExecutionException
        {
            if (response.getMessageType().equals( initiatedByMessageType.failureMessage() ))
            {
                // Call timed out
                if (response.getPayload() != null)
                {
                    if (response.getPayload() instanceof Throwable)
                    {
                        throw new ExecutionException( (Throwable) response.getPayload() );
                    } else
                    {
                        throw new InterruptedException( response.getPayload().toString() );
                    }
                } else
                {
                    // No message specified
                    throw new InterruptedException(  );
                }
            } else
            {
                // Return result
                return response.getPayload();
            }
        }

        @Override
        public Object get( long timeout, TimeUnit unit )
            throws InterruptedException, ExecutionException, TimeoutException
        {
            if (response != null)
            {
                getResult();
            }

            this.wait(unit.toMillis( timeout ));
            
            return getResult();
        }
    }
}
