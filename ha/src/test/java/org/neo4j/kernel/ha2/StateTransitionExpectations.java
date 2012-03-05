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
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.ha2.statemachine.State;
import org.neo4j.kernel.ha2.statemachine.StateTransition;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;
import org.neo4j.kernel.ha2.statemachine.message.MessageType;

public class StateTransitionExpectations<CONTEXT,E extends Enum<E>>
{
    public static final StateTransitionListener NO_EXPECTATIONS = new StateTransitionListener()
    {
        @Override
        public void stateTransition( StateTransition transition )
        {
        }
    };
    
    private final List<ExpectingStateTransitionListener> expectations = new ArrayList<ExpectingStateTransitionListener>();
    
    public ExpectationsBuilder newExpectations()
    {
        return new ExpectationsBuilder();
    }
    
    public void verify()
    {
        for ( ExpectingStateTransitionListener listener : expectations )
            listener.verify();
    }
    
    public class ExpectationsBuilder
    {
        private final Deque<ExpectedTransition> transitions = new LinkedList<ExpectedTransition>();
        private boolean includeUnchanged;
        
        public ExpectationsBuilder expect( MessageType messageToGetHere, State<CONTEXT,E> state )
        {
            transitions.add( new ExpectedTransition( messageToGetHere, state ) );
            return this;
        }
        
        public ExpectationsBuilder includeUnchangedStates()
        {
            this.includeUnchanged = true;
            return this;
        }
        
        public StateTransitionListener build( Object id )
        {
            ExpectingStateTransitionListener listener = new ExpectingStateTransitionListener( new LinkedList<ExpectedTransition>( transitions ), includeUnchanged, id );
            expectations.add( listener );
            return listener;
        }
        
        public void assertNoMoreExpectations()
        {
            if ( !transitions.isEmpty() )
                throw new IllegalStateException( "Unsatisfied transitions: " + transitions );
        }
    }
    
    private class ExpectingStateTransitionListener implements StateTransitionListener
    {
        private final Deque<ExpectedTransition> transitions;
        private volatile IllegalStateException mostRecentException;
        private final Object id;
        private final boolean includeUnchanged;

        ExpectingStateTransitionListener( Deque<ExpectedTransition> transitions, boolean includeUnchanged, Object id )
        {
            this.transitions = transitions;
            this.includeUnchanged = includeUnchanged;
            this.id = id;
        }

        @Override
        public void stateTransition( StateTransition transition )
        {
            try
            {
                if ( !includeUnchanged && transition.getOldState().equals( transition.getNewState() ) )
                    return;
                
                if ( transitions.isEmpty() )
                    throw new IllegalStateException( message( "No more transactions expected, but got " + transition ) );
                
                ExpectedTransition expected = transitions.pop();
                if ( !expected.matches( transition ) )
                    throw new IllegalStateException( message( "Expected " + expected + ", but got " + transition ) );
            }
            catch ( IllegalStateException e )
            {
                mostRecentException = e;
                throw e;
            }
        }
        
        private String message( String string )
        {
            return "[" + id + "]: " + string;
        }

        void verify()
        {
            if ( mostRecentException != null )
                throw mostRecentException;
            if ( !transitions.isEmpty() )
                throw new IllegalStateException( message( "Expected transactions not encountered: " + transitions ) );
        }
    }
    
    private class ExpectedTransition
    {
        private final MessageType messageToGetHere;
        private final State<CONTEXT,E> state;
        
        ExpectedTransition( MessageType messageToGetHere, State<CONTEXT, E> state )
        {
            this.messageToGetHere = messageToGetHere;
            this.state = state;
        }

        public boolean matches( StateTransition transition )
        {
            return state.equals( transition.getNewState() ) && messageToGetHere.equals( transition.getMessage().getMessageType() );
        }
        
        @Override
        public String toString()
        {
            return messageToGetHere + "->" + state;
        }
    }
}
