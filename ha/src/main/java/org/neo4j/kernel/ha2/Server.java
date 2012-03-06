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
import java.util.logging.Logger;
import org.neo4j.com2.NetworkChannels;
import org.neo4j.com2.NetworkMessageReceiver;
import org.neo4j.com2.NetworkMessageSender;
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
public class Server
    implements Lifecycle
{

    protected final StateMachine<TokenRingContext, TokenRingMessage> stateMachine;
    protected final StateMachineConversations conversations;
    protected final TokenRingContext context;
    protected final NetworkMessageReceiver receiver;
    protected final NetworkMessageSender sender;
    protected StateMachineProxyFactory proxyFactory;
    protected final NetworkedStateMachine networkedStateMachine;

    public interface Configuration
        extends NetworkMessageReceiver.Configuration
    {
    }

    private Logger logger = Logger.getLogger( getClass().getName() );

    private final LifeSupport life = new LifeSupport();
    private TokenRing tokenRing;
    private Configuration config;

    public Server( Configuration config )
    {
        this.config = config;

        context = new TokenRingContext();
        stateMachine = new StateMachine<TokenRingContext, TokenRingMessage>( context, TokenRingMessage.class, TokenRingState.start );
        conversations = new StateMachineConversations();

        final NetworkChannels channels = new NetworkChannels( StringLogger.SYSTEM );
        channels.addNetworkChannelsListener( new NetworkChannels.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                RingParticipant participant = new RingParticipant( me.toString() );
                context.setMe( participant );
                networkedStateMachine.setMe( participant );

                stateMachine.addStateTransitionListener( new StateTransitionLogger( participant, Logger.getAnonymousLogger() ) );

                proxyFactory = new StateMachineProxyFactory( participant.getServerId(), TokenRingMessage.class, stateMachine, networkedStateMachine, conversations );
                networkedStateMachine.addOutgoingProcessor( proxyFactory );

            }

            @Override
            public void channelOpened( URI to )
            {
            }

            @Override
            public void channelClosed( URI to )
            {
            }
        } );

        receiver = new NetworkMessageReceiver( config, StringLogger.SYSTEM, channels );
        sender = new NetworkMessageSender( StringLogger.SYSTEM, channels );
        networkedStateMachine = new NetworkedStateMachine( receiver, sender, stateMachine );

        life.add( receiver );
        life.add( sender );
        life.add( networkedStateMachine );
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
        life.start();

        tokenRing = proxyFactory.newProxy( TokenRing.class );
        tokenRing.start();

        try
        {
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void stop()
        throws Throwable
    {
        logger.info( "Stop server" );
        life.stop();
    }

    @Override
    public void shutdown()
        throws Throwable
    {
    }

    public <T> T newClient( Class<T> clientProxyInterface )
    {
        return proxyFactory.newProxy(clientProxyInterface);
    }
}
