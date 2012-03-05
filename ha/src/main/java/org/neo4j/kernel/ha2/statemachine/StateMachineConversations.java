/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.ha2.statemachine;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.neo4j.kernel.ha2.statemachine.message.InternalMessage;
import org.neo4j.kernel.ha2.statemachine.message.MessageType;

/**
 * TODO
 */
public class StateMachineConversations
{
    private String prefix;
    private Class<? extends Enum> messageTypeEnum;
    private long nextConversationId = 0;

    public StateMachineConversations( String prefix, Class<? extends Enum> messageTypeEnum)
    {
        this.prefix = prefix;
        this.messageTypeEnum = messageTypeEnum;
    }

    public Object invoke( StateMachine stateMachine, Method method, Object arg )
        throws Throwable
    {
        String conversationId = prefix+getNextConversationId();

        Enum typeAsEnum = Enum.valueOf( messageTypeEnum, method.getName() );
        StateMessage stateMessage = new StateMessage( conversationId, new InternalMessage( (MessageType) typeAsEnum, arg ) );

        if (method.getReturnType().equals( Void.TYPE ))
        {
            stateMachine.receive( stateMessage );
            return null;
        }
        else
        {
            ResponseFuture future = new ResponseFuture();
            stateMachine.addStateTransitionListener( new ResponseListener(conversationId, future) );
            stateMachine.receive( stateMessage );

            try
            {
                // Wait for response or timeout/failure
                return future.get(  );
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

    public synchronized long getNextConversationId()
    {
        return nextConversationId++;
    }

    class ResponseListener
        implements StateTransitionListener
    {
        private String conversationId;
        private ResponseFuture future;

        public ResponseListener( String conversationId, ResponseFuture future )
        {
            this.conversationId = conversationId;
            this.future = future;
        }

        @Override
        public void stateTransition( StateTransition transition )
        {
            if (transition.getMessage().getConversationId().equals( conversationId ))
            {
                future.setResponse( transition.getMessage().getMessage().getPayload() );
            }
        }
    }
    
    class ResponseFuture
        implements Future
    {
        private Object response;
        private Exception exception;

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
