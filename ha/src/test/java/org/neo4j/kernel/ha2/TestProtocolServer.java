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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageSource;
import org.neo4j.com_2.message.MessageType;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;
import org.neo4j.kernel.ha2.statemachine.StateTransitionLogger;
import org.neo4j.kernel.ha2.timeout.TestTimeouts;
import org.neo4j.kernel.ha2.timeout.TimeoutStrategy;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public class TestProtocolServer
    implements MessageProcessor
{
    protected final TestMessageSource receiver;
    protected final TestMessageSender sender;
    protected TestTimeouts timeouts;

    private Logger logger = Logger.getLogger( getClass().getName() );

    private final LifeSupport life = new LifeSupport();
    protected ProtocolServer server;

    public TestProtocolServer( TimeoutStrategy timeoutStrategy, ProtocolServerFactory factory, String serverId )
    {
        this.receiver = new TestMessageSource();
        this.sender = new TestMessageSender();
        this.timeouts = new TestTimeouts( receiver, timeoutStrategy );

        server = factory.newProtocolServer( timeouts, receiver, sender );

        server.addStateTransitionListener( new StateTransitionLogger( serverId, LoggerFactory.getLogger(StateTransitionLogger.class) ) );

        try
        {
            server.listeningAt( new URI(serverId) );
        } catch (URISyntaxException e)
        {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        life.add( server );
    }

    public ProtocolServer getServer()
    {
        return server;
    }

    public TestTimeouts getTimeouts()
    {
        return timeouts;
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
    
    public TestProtocolServer addStateTransitionListener(StateTransitionListener listener)
    {
        server.addStateTransitionListener( listener );
        return this;
    }

    public void tick(long time)
    {
        // Time passes - check timeouts
        timeouts.tick( time );
    }

    @Override
    public String toString()
    {
        return server.getServerId()+": "+sender.getMessages().size()+server.toString();
    }

    public class TestMessageSender
        implements MessageProcessor
    {
        List<Message> messages = new ArrayList<Message>(  );
        
        @Override
        public void process( Message<? extends MessageType> message )
        {
            messages.add( message );
        }

        public List<Message> getMessages()
        {
            return messages;
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
        public void process( Message<? extends MessageType> message )
        {
            for( MessageProcessor listener : listeners )
            {
                listener.process( message );
            }
        }
    }
}
