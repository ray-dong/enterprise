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

package org.neo4j.kernel.ha2;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;
import org.neo4j.com2.MessageReceiver;
import org.neo4j.com2.MessageSender;
import org.neo4j.com2.NetworkChannels;
import org.neo4j.com2.NetworkMessageListener;
import org.neo4j.com2.NetworkMessageReceiver;
import org.neo4j.com2.NetworkMessageSender;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.LifeSupport;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRing;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingContext;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.statemachine.StateMachineConversations;
import org.neo4j.kernel.ha2.statemachine.StateMachineProxyFactory;
import org.neo4j.kernel.ha2.statemachine.StateTransitionLogger;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class TestServer
{

    protected final StateMachine<TokenRingContext, TokenRingMessage> stateMachine;
    protected final StateMachineConversations conversations;
    protected final TokenRingContext context;
    protected final TestMessageReceiver receiver;
    protected final TestMessageSender sender;
    protected StateMachineProxyFactory proxyFactory;
    protected final NetworkedStateMachine networkedStateMachine;

    private Logger logger = Logger.getLogger( getClass().getName() );

    private final LifeSupport life = new LifeSupport();
    private TokenRing tokenRing;
    
    Iterable<NetworkMessageListener> listeners = Listeners.newListeners();

    public TestServer( String serverId )
    {
        RingParticipant participant = new RingParticipant( serverId );
        
        context = new TokenRingContext();
        stateMachine = new StateMachine<TokenRingContext, TokenRingMessage>( context, TokenRingMessage.class, TokenRingState.start );
        conversations = new StateMachineConversations();

        this.receiver = new TestMessageReceiver();
        this.sender = new TestMessageSender();
        networkedStateMachine = new NetworkedStateMachine( receiver, sender, stateMachine );

        stateMachine.addStateTransitionListener( new StateTransitionLogger( participant, Logger.getAnonymousLogger() ) );

        proxyFactory = new StateMachineProxyFactory( participant.getServerId(), TokenRingMessage.class, stateMachine, networkedStateMachine, conversations );
        networkedStateMachine.addOutgoingProcessor( proxyFactory );

        context.setMe( participant );
        networkedStateMachine.setMe( participant );


        life.add( networkedStateMachine );

        start();
    }

    public void receive( Object message )
    {
        for( NetworkMessageListener listener : listeners )
        {
            listener.received( message );
        }
    }
    
    public void sendMessages(List<TestMessage> output)
    {
        sender.sendMessages( output );
    }
    
    public void start()
    {
        life.start();

        tokenRing = proxyFactory.newProxy( TokenRing.class );
        tokenRing.start();
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

    public class TestMessageSender
        implements MessageSender
    {
        List<TestMessage> messages = new ArrayList<TestMessage>(  );
        
        @Override
        public void send( String to, Object message )
        {
            messages.add( new TestMessage(to, message) );
        }

        public void sendMessages( List<TestMessage> output )
        {
            output.addAll( messages );
            messages.clear();
        }
    }
    
    public class TestMessageReceiver
        implements MessageReceiver
    {
        Iterable<NetworkMessageListener> listeners = Listeners.newListeners();
        
        @Override
        public void addMessageListener( NetworkMessageListener listener )
        {
            listeners = Listeners.addListener( listener, listeners );
        }
        
        public void process(Object message)
        {
            for( NetworkMessageListener listener : listeners )
            {
                listener.received( message );
            }
        }
    }
    
    public class TestMessage
    {
        private String to;
        private Object message;

        public TestMessage( String to, Object message )
        {
            this.to = to;
            this.message = message;
        }

        public String getTo()
        {
            return to;
        }

        public Object getMessage()
        {
            return message;
        }
    }
}
