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
import java.util.ArrayList;
import java.util.List;
import org.neo4j.kernel.ha2.statemachine.message.Message;

/**
 * TODO
 */
public class StateMachine<CONTEXT, E extends Enum>
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
    
    public void checkValidProxyInterface(Class<?> proxyInterface)
        throws IllegalArgumentException
    {
        for( Method method : proxyInterface.getMethods() )
        {
            Enum.valueOf( messageEnumType, method.getName() );
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

    public synchronized void receive(Message message)
    {
        try
        {
            State<CONTEXT,E> oldState = state;
            State<CONTEXT,E> newState = state.receive(context, message);
            state = newState;
            StateTransition transition = new StateTransition( oldState, message, newState );
            for (StateTransitionListener listener : listeners)
            {
                listener.stateTransition(transition);
            }

        } catch (IllegalStateException throwable)
        {
            System.out.println(throwable.getMessage());
        } catch (Throwable throwable)
        {
            throwable.printStackTrace();
        }
    }
}