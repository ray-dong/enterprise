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
import org.neo4j.com2.message.MessageType;

/**
 * TODO
 */
public class StateMachine<CONTEXT, MESSAGETYPE extends Enum<MESSAGETYPE> & MessageType, STATE extends State<CONTEXT, MESSAGETYPE, STATE>>
{
    private CONTEXT context;
    private Class<MESSAGETYPE> messageEnumType;
    private STATE state;

    private List<StateTransitionListener<MESSAGETYPE>> listeners = new ArrayList<StateTransitionListener<MESSAGETYPE>>();

    public StateMachine(CONTEXT context, Class<MESSAGETYPE> messageEnumType, STATE state)
    {
        this.context = context;
        this.messageEnumType = messageEnumType;
        this.state = state;
    }

    public Class<MESSAGETYPE> getMessageType()
    {
        return messageEnumType;
    }

    public CONTEXT getContext()
    {
        return context;
    }

    public STATE getState()
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

    public void addStateTransitionListener( StateTransitionListener<MESSAGETYPE> listener
    )
    {
        List<StateTransitionListener<MESSAGETYPE>> newlisteners = new ArrayList<StateTransitionListener<MESSAGETYPE>>(listeners);
        newlisteners.add( listener );
        listeners = newlisteners;
    }

    public void removeStateTransitionListener(StateTransitionListener<MESSAGETYPE> listener)
    {
        List<StateTransitionListener<MESSAGETYPE>> newlisteners = new ArrayList<StateTransitionListener<MESSAGETYPE>>(listeners);
        newlisteners.remove(listener);
        listeners = newlisteners;
    }

    public synchronized void handle( Message<MESSAGETYPE> message, MessageProcessor outgoing )
    {
        try
        {
            STATE oldState = state;
            STATE newState = state.handle( context, message, outgoing );
            state = newState;

            StateTransition<MESSAGETYPE> transition = new StateTransition<MESSAGETYPE>( oldState, message, newState );
            for (StateTransitionListener<MESSAGETYPE> listener : listeners)
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