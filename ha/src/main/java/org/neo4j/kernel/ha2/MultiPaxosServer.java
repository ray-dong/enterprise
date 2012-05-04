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

import java.util.logging.Logger;
import org.neo4j.com2.NetworkNode;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.MultiPaxosContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.statemachine.StateMachineConversations;
import org.neo4j.kernel.ha2.statemachine.StateMachineProxyFactory;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;

/**
 * TODO
 */
public class MultiPaxosServer
    extends ProtocolServer<MultiPaxosContext,AtomicBroadcastMessage>
{
    protected final StateMachine<MultiPaxosContext, AcceptorMessage, AcceptorState> acceptor;
    protected final StateMachine<MultiPaxosContext, ProposerMessage, ProposerState> coordinator;
    protected final StateMachine<MultiPaxosContext, LearnerMessage, LearnerState> learner;
    protected final StateMachine<MultiPaxosContext, AtomicBroadcastMessage, AtomicBroadcastState> paxosStateMachine;

    public interface Configuration
        extends NetworkNode.Configuration
    {
    }

    private Logger logger = Logger.getLogger( getClass().getName() );

    public MultiPaxosServer( MultiPaxosContext context,
                             MessageSource input,
                             MessageProcessor output
    )
    {
        super(context, input, output);

        acceptor = new StateMachine<MultiPaxosContext, AcceptorMessage, AcceptorState>(context, AcceptorMessage.class, AcceptorState.start);
        coordinator = new StateMachine<MultiPaxosContext, ProposerMessage, ProposerState>(context, ProposerMessage.class, ProposerState.start);
        learner = new StateMachine<MultiPaxosContext, LearnerMessage, LearnerState>(context, LearnerMessage.class, LearnerState.start);
        paxosStateMachine = new StateMachine<MultiPaxosContext, AtomicBroadcastMessage, AtomicBroadcastState>(context, AtomicBroadcastMessage.class, AtomicBroadcastState.start);

        connectedStateMachines.addMessageProcessor( new FromHeaderMessageProcessor() );
        connectedStateMachines.addStateMachine( acceptor );
        connectedStateMachines.addStateMachine( coordinator );
        connectedStateMachines.addStateMachine( learner );
        connectedStateMachines.addStateMachine( paxosStateMachine );
    }
    
    @Override
    public void start()
        throws Throwable
    {
/*
        Class<AtomicBroadcast> proxyInterface = AtomicBroadcast.class;
        AtomicBroadcast atomicBroadcast = proxyFactory.newProxy( proxyInterface );
        atomicBroadcast.join();
*/
    }


    public void listeningAt( String me )
    {
        logger.info( "=== " + me + " starts" );

        context.setMe( me );

//        paxosStateMachine.addStateTransitionListener( new StateTransitionLogger<AtomicBroadcastMessage>( me, Logger.getAnonymousLogger() ) );

        StateMachineConversations conversations = new StateMachineConversations(me);
        proxyFactory = new StateMachineProxyFactory<AtomicBroadcastMessage>( me, AtomicBroadcastMessage.class, paxosStateMachine, connectedStateMachines, conversations );
        connectedStateMachines.addMessageProcessor( proxyFactory );

        super.listeningAt( me );
    }
    
    public void addStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        acceptor.addStateTransitionListener( stateTransitionListener );
        coordinator.addStateTransitionListener( stateTransitionListener );
        learner.addStateTransitionListener( stateTransitionListener );
        paxosStateMachine.addStateTransitionListener( stateTransitionListener );
    }

    public <T> T newClient( Class<T> clientProxyInterface )
    {
        return proxyFactory.newProxy( clientProxyInterface );
    }

    private class FromHeaderMessageProcessor
        implements MessageProcessor
    {
        @Override
        public void process( Message message )
        {
            if( message.hasHeader( Message.TO ) )
            {
                message.setHeader( Message.FROM, context.getMe() );
            }
        }
    }
}
