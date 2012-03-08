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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;

/**
 * TODO
 */
public class StateMachine<CONTEXT, E extends Enum<E>>
{
    private CONTEXT context;
    private Class<E> messageEnumType;
    private State<CONTEXT,E> state;

    private List<StateTransitionListener> listeners = new ArrayList<StateTransitionListener>();

    public StateMachine(CONTEXT context, Class<E> messageEnumType, State<CONTEXT,E> state)
    {
        this.context = context;
        this.messageEnumType = messageEnumType;
        this.state = state;
    }

    public CONTEXT getContext()
    {
        return context;
    }

    public State<CONTEXT, E> getState()
    {
        return state;
    }

    public void checkValidProxyInterface(Class<?> proxyInterface)
        throws IllegalArgumentException
    {
        for( Method method : proxyInterface.getMethods() )
        {
            Enum.valueOf( messageEnumType, method.getName() );
            
            if (!(method.getReturnType().equals( Void.TYPE ) || method.getReturnType().equals( Future.class )))
            {
                throw new IllegalArgumentException( "Methods must return either void or Future" );
            }
        }
    }

    public void addStateTransitionListener( StateTransitionListener listener
    )
    {
        List<StateTransitionListener> newlisteners = new ArrayList<StateTransitionListener>(listeners);
        newlisteners.add( listener );
        listeners = newlisteners;
    }

    public void removeStateTransitionListener(StateTransitionListener listener)
    {
        List<StateTransitionListener> newlisteners = new ArrayList<StateTransitionListener>(listeners);
        newlisteners.remove(listener);
        listeners = newlisteners;
    }

    public synchronized void handle( Message message, MessageProcessor outgoing )
    {
        try
        {
            State<CONTEXT,E> oldState = state;
            State<CONTEXT,E> newState = state.handle( context, message, outgoing );
            state = newState;

            StateTransition transition = new StateTransition( oldState, message, newState );
            for (StateTransitionListener listener : listeners)
            {
                try
                {
                    listener.stateTransition(transition);
                }
                catch( Throwable e )
                {
                    // Ignore
                    e.printStackTrace(  );
                }
            }

        } catch (Throwable throwable)
        {
            throwable.printStackTrace();
        }
    }
}