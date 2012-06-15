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

import java.util.ArrayList;
import java.util.List;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageType;

/**
 * State machine that wraps a context and a state, which can change when a Message comes in.
 * Incoming messages must be of the particular type that the state understands.
 */
public class StateMachine
{
    private Object context;
    private Class<? extends MessageType> messageEnumType;
    private State<?,?> state;

    private List<StateTransitionListener> listeners = new ArrayList<StateTransitionListener>();

    public StateMachine(Object context, Class<? extends MessageType> messageEnumType, State<?,?> state)
    {
        this.context = context;
        this.messageEnumType = messageEnumType;
        this.state = state;
    }

    public Class<? extends MessageType> getMessageType()
    {
        return messageEnumType;
    }

    public State<?,?> getState()
    {
        return state;
    }

    public Object getContext()
    {
        return context;
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

    public synchronized void handle( Message<? extends MessageType> message, MessageProcessor outgoing )
    {
        try
        {
            State<Object,MessageType> oldState = (State<Object, MessageType>) state;
            State<?,?> newState = oldState.handle( (Object) context, (Message<MessageType>) message, outgoing );
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

    @Override
    public String toString()
    {
        return state.toString()+": "+context.toString();
    }
}