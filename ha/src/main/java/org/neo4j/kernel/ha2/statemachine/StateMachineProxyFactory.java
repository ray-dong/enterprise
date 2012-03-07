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
public class StateMachineProxyFactory
    implements MessageProcessor
{
    private String prefix;
    private StateMachine stateMachine;
    private MessageProcessor incoming;
    private StateMachineConversations conversations;
    private Class<? extends Enum> messageTypeEnum;

    private Map<String, ResponseFuture> responseFutureMap = new ConcurrentHashMap<String, ResponseFuture>(  );
    
    
    public StateMachineProxyFactory( String prefix, Class<? extends Enum> messageTypeEnum, StateMachine stateMachine, MessageProcessor incoming, StateMachineConversations conversations )
    {
        this.prefix = prefix;
        this.messageTypeEnum = messageTypeEnum;
        this.stateMachine = stateMachine;
        this.incoming = incoming;
        this.conversations = conversations;
    }
    
    public <T> T newProxy(Class<T> proxyInterface)
    {
        stateMachine.checkValidProxyInterface( proxyInterface );

        return proxyInterface.cast( Proxy.newProxyInstance( proxyInterface.getClassLoader(), new Class<?>[]{ proxyInterface }, new StateMachineProxyHandler( this ) ) );
    }

    Object invoke( Method method, Object arg )
        throws Throwable
    {
        String conversationId = prefix+"/"+conversations.getNextConversationId();

        Enum typeAsEnum = Enum.valueOf( messageTypeEnum, method.getName() );
        Message message = Message.internal( (MessageType) typeAsEnum, arg ).setHeader( Message.CONVERSATION_ID, conversationId ).setHeader( Message.CREATED_BY, prefix );

        if (method.getReturnType().equals( Void.TYPE ))
        {
            incoming.process( message );
            return null;
        }
        else
        {
            ResponseFuture future = new ResponseFuture(method.getName());
            responseFutureMap.put( conversationId, future );
            incoming.process( message );

            try
            {
                if (method.getReturnType().equals( Future.class ))
                {
                    // Return the future and let client decide on how to wait
                    return future;
                }
                else
                {
                    // Wait for response or timeout/failure
                    return future.get(  );
                }
            }
            catch( InterruptedException e )
            {
                throw e;
            }
            catch( ExecutionException e )
            {
                throw e.getCause();
            }
        }
    }

    @Override
    public void process( Message message )
    {
        if (!message.hasHeader( Message.TO ) && !responseFutureMap.isEmpty())
        {
            String conversationId = message.getHeader( Message.CONVERSATION_ID );
            ResponseFuture future = responseFutureMap.get( conversationId );
            if (future != null && !future.wasInitiatedBy(message.getMessageType().name()))
            {
                future.setResponse( message.getPayload() );
                responseFutureMap.remove( conversationId );
            }
        }
    }

    class ResponseFuture
        implements Future
    {
        private String initiatedByMessageType;
        private Object response;
        private Exception exception;

        ResponseFuture( String initiatedByMessageType )
        {
            this.initiatedByMessageType = initiatedByMessageType;
        }

        public boolean wasInitiatedBy( String name )
        {
            return initiatedByMessageType.equals( name );
        }

        public synchronized void setResponse( Object response )
        {
            this.response = response;
            this.notifyAll();
        }

        public synchronized void setException( Exception exception )
        {
            this.exception = exception;
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
            return response != null || exception != null;
        }

        @Override
        public synchronized Object get()
            throws InterruptedException, ExecutionException
        {
            if (response != null)
                return response;
            
            if (exception != null)
                throw new ExecutionException( exception );
            
            this.wait();
            
            return get( );
        }

        @Override
        public Object get( long timeout, TimeUnit unit )
            throws InterruptedException, ExecutionException, TimeoutException
        {
            if (response != null)
                return response;

            if (exception != null)
                throw new ExecutionException( exception );
            
            this.wait(unit.toMillis( timeout ));
            
            return get( timeout, unit);
        }
    }
}
