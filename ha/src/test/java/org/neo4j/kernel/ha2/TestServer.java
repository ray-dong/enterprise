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
import org.neo4j.kernel.ha2.protocol.tokenring.ServerIdRingParticipantComparator;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingContext;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;

/**
 * TODO
 */
public class TestServer
    implements MessageProcessor
{
    protected final TestMessageSource receiver;
    protected final TestMessageSender sender;
    protected TestMessageFailureHandler failureHandler;

    private Logger logger = Logger.getLogger( getClass().getName() );

    private final LifeSupport life = new LifeSupport();
    protected final Server server;
    protected final TokenRingContext tokenRingContext;

    public TestServer( String serverId )
    {
        RingParticipant participant = new RingParticipant( serverId );
        
        this.receiver = new TestMessageSource();
        this.sender = new TestMessageSender();

        tokenRingContext = new TokenRingContext(new ServerIdRingParticipantComparator());
        server = new Server( tokenRingContext, receiver, sender, new TestFailureHandlerFactory() );
        
        server.listeningAt( participant );

        life.add( server );
    }

    public Server getServer()
    {
        return server;
    }

    public void verifyState( Verifier<TokenRingContext> verifier )
    {
        verifier.verify( tokenRingContext );
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
        return server.newClient( clientProxyInterface );
    }
    
    public TestServer addStateTransitionListener(StateTransitionListener listener)
    {
        server.addStateTransitionListener( listener );
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

    private class TestFailureHandlerFactory
        implements AbstractMessageFailureHandler.Factory
    {
        @Override
        public AbstractMessageFailureHandler newMessageFailureHandler( MessageProcessor incoming,
                                                                       MessageSource outgoing,
                                                                       MessageSource source
        )
        {
            return failureHandler = new TestMessageFailureHandler( incoming, outgoing, receiver);
        }
    }
}
