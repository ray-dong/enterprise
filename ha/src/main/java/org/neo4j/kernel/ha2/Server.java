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
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.neo4j.com2.NetworkNode;
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
    protected final TokenRingContext context;
    protected final NetworkNode node;
    protected StateMachineProxyFactory proxyFactory;
    protected final NetworkedStateMachine networkedStateMachine;

    public interface Configuration
        extends NetworkNode.Configuration
    {
    }

    private Logger logger = Logger.getLogger( getClass().getName() );

    private final LifeSupport life = new LifeSupport();
    private TokenRing tokenRing;

    public Server( Configuration config )
    {
        context = new TokenRingContext();
        stateMachine = new StateMachine<TokenRingContext, TokenRingMessage>( context, TokenRingMessage.class, TokenRingState.start );

        node = new NetworkNode( config, StringLogger.SYSTEM );
        node.addNetworkChannelsListener( new NetworkNode.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                RingParticipant participant = new RingParticipant( me.toString() );
                context.setMe( participant );
                networkedStateMachine.setMe( participant );

                stateMachine.addStateTransitionListener( new StateTransitionLogger( participant, Logger.getAnonymousLogger() ) );

                StateMachineConversations conversations = new StateMachineConversations(participant.getServerId());
                proxyFactory = new StateMachineProxyFactory( participant.getServerId(), TokenRingMessage.class, stateMachine, networkedStateMachine, conversations );
                networkedStateMachine.addMessageProcessor( proxyFactory );

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

        networkedStateMachine = new NetworkedStateMachine( node, node, stateMachine );
        life.add(new TimeoutMessageFailureHandler( networkedStateMachine, networkedStateMachine, node,
                                                   new FixedTimeoutStrategy(TimeUnit.SECONDS.toMillis( 10 )) ));

        life.add( node );
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
            Thread.sleep(2000);
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
