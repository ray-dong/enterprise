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
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.ha2.failure.AbstractMessageFailureHandler;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRing;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingContext;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.statemachine.StateMachineConversations;
import org.neo4j.kernel.ha2.statemachine.StateMachineProxyFactory;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;
import org.neo4j.kernel.ha2.statemachine.StateTransitionLogger;

/**
 * TODO
 */
public class Server
    implements Lifecycle
{
    protected final StateMachine<TokenRingContext, TokenRingMessage, TokenRingState> stateMachine;
    protected final TokenRingContext context;
    protected StateMachineProxyFactory<TokenRingMessage> proxyFactory;
    protected final ConnectedStateMachines connectedStateMachines;

    public interface Configuration
        extends NetworkNode.Configuration
    {
    }

    private Logger logger = Logger.getLogger( getClass().getName() );

    public Server( TokenRingContext context, MessageSource input, MessageProcessor output, AbstractMessageFailureHandler.Factory failureHandlerFactory )
    {
        this.context = context;
        stateMachine = new StateMachine<TokenRingContext, TokenRingMessage, TokenRingState>( context, TokenRingMessage.class, TokenRingState.start );
        connectedStateMachines = new ConnectedStateMachines( input, output );
        connectedStateMachines.addMessageProcessor( new FromHeaderMessageProcessor() );
        connectedStateMachines.addStateMachine( stateMachine );
        failureHandlerFactory.newMessageFailureHandler( connectedStateMachines, connectedStateMachines, input );
    }
    
    @Override
    public void init()
        throws Throwable
    {
    }

    @Override
    public void start()
        throws Throwable
    {
        Class<TokenRing> proxyInterface = TokenRing.class;
        TokenRing tokenRing = proxyFactory.newProxy( proxyInterface );
        tokenRing.joinRing();
    }

    @Override
    public void stop()
        throws Throwable
    {
        logger.info( "Stop server" );
    }

    @Override
    public void shutdown()
        throws Throwable
    {
    }
    
    public void listeningAt( RingParticipant participant )
    {
        logger.info( "=== " + participant + " starts" );

        context.setMe( participant );

        stateMachine.addStateTransitionListener( new StateTransitionLogger<TokenRingMessage>( participant, Logger.getAnonymousLogger() ) );

        StateMachineConversations conversations = new StateMachineConversations(participant.getServerId());
        proxyFactory = new StateMachineProxyFactory<TokenRingMessage>( participant.getServerId(), TokenRingMessage.class, stateMachine, connectedStateMachines, conversations );
        connectedStateMachines.addMessageProcessor( proxyFactory );

    }
    
    /**
     * Ok to have this accessible like this?
     * @return server id
     */
    public String getServerId()
    {
        return context.getMe().getServerId();
    }

    public TokenRingContext getContext()
    {
        return context;
    }

    public TokenRingState getState()
    {
        return stateMachine.getState();
    }

    public void addStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        stateMachine.addStateTransitionListener( stateTransitionListener );
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
                message.setHeader( Message.FROM, context.getMe().getServerId() );
            }
        }
    }
}
