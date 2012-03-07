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
import java.util.List;
import java.util.logging.Logger;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.LifeSupport;
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
public class TestServer
    implements MessageProcessor
{
    protected final StateMachine<TokenRingContext, TokenRingMessage> stateMachine;
    protected final StateMachineConversations conversations;
    protected final TokenRingContext context;
    protected final TestMessageSource receiver;
    protected final TestMessageSender sender;
    protected final StateMachineProxyFactory proxyFactory;
    protected final NetworkedStateMachine networkedStateMachine;
    protected final TestMessageFailureHandler failureHandler;

    private Logger logger = Logger.getLogger( getClass().getName() );

    private final LifeSupport life = new LifeSupport();
    private TokenRing tokenRing;
    
    Iterable<MessageProcessor> listeners = Listeners.newListeners();

    public TestServer( String serverId )
    {
        RingParticipant participant = new RingParticipant( serverId );
        
        context = new TokenRingContext();
        stateMachine = new StateMachine<TokenRingContext, TokenRingMessage>( context, TokenRingMessage.class, TokenRingState.start );
        conversations = new StateMachineConversations(serverId);

        this.receiver = new TestMessageSource();
        this.sender = new TestMessageSender();
        networkedStateMachine = new NetworkedStateMachine( receiver, sender, stateMachine );

        stateMachine.addStateTransitionListener( new StateTransitionLogger( participant, Logger.getAnonymousLogger() ) );

        proxyFactory = new StateMachineProxyFactory( serverId, TokenRingMessage.class, stateMachine, networkedStateMachine, conversations );
        networkedStateMachine.addMessageProcessor( proxyFactory );

        failureHandler = new TestMessageFailureHandler(networkedStateMachine, networkedStateMachine, receiver);
        
        context.setMe( participant );
        networkedStateMachine.setMe( participant );

        
        life.add( networkedStateMachine );

        start();
    }
    
    public void verifyState( Verifier<TokenRingContext> verifier )
    {
        verifier.verify( context );
    }

    @Override
    public void process( Message message )
    {
        receiver.process( message );
    }
    
    public void sendMessages(List<Message> output)
    {
        sender.sendMessages( output );
    }
    
    public void start()
    {
        life.start();
    }

    public void stop()
    {
        logger.info( "Stop server" );
        life.stop();
    }

    public <T> T newClient( Class<T> clientProxyInterface )
    {
        return proxyFactory.newProxy(clientProxyInterface);
    }
    
    public TestServer addStateTransitionListener(StateTransitionListener listener)
    {
        stateMachine.addStateTransitionListener( listener );
        return this;
    }

    public void checkExpectations()
    {
        failureHandler.checkExpectations();
    }

    public class TestMessageSender
        implements MessageProcessor
    {
        List<Message> messages = new ArrayList<Message>(  );
        
        @Override
        public void process( Message message )
        {
            messages.add( message );
        }

        public void sendMessages( List<Message> output )
        {
            output.addAll( messages );
            messages.clear();
        }
    }
    
    public class TestMessageSource
        implements MessageSource, MessageProcessor
    {
        Iterable<MessageProcessor> listeners = Listeners.newListeners();

        @Override
        public void addMessageProcessor( MessageProcessor listener )
        {
            listeners = Listeners.addListener( listener, listeners );
        }

        @Override
        public void process( Message message )
        {
            for( MessageProcessor listener : listeners )
            {
                listener.process( message );
            }
        }
    }

    public class TestMessageFailureHandler
        extends AbstractMessageFailureHandler
    {
        public TestMessageFailureHandler( MessageProcessor incoming, MessageSource outgoing, MessageSource source )
        {
            super(incoming, outgoing, source);
        }

        public void checkExpectations()
        {
            for( ExpectationFailure expectationFailure : expectations.values() )
            {
                expectationFailure.run();
            }
        }
    }
}
