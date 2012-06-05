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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.neo4j.com_2.NetworkNode;
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.ha2.statemachine.StateTransitionLogger;
import org.neo4j.kernel.ha2.timeout.TimeoutStrategy;
import org.neo4j.kernel.ha2.timeout.Timeouts;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public class NetworkedServerFactory
{
    private LifeSupport life;
    private ProtocolServerFactory protocolServerFactory;
    private TimeoutStrategy timeoutStrategy;
    private StringLogger logger;

    public NetworkedServerFactory( LifeSupport life, ProtocolServerFactory protocolServerFactory, TimeoutStrategy timeoutStrategy, StringLogger logger )
    {
        this.life = life;
        this.protocolServerFactory = protocolServerFactory;
        this.timeoutStrategy = timeoutStrategy;
        this.logger = logger;
    }

    public ProtocolServer newNetworkedServer(final Map<String, String> configuration)
    {
        final NetworkNode node = new NetworkNode( configuration, logger );
        life.add( node );

        final ProtocolServer protocolServer = protocolServerFactory.newProtocolServer(timeoutStrategy, node, node);
        node.addNetworkChannelsListener( new NetworkNode.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                protocolServer.listeningAt( me );
                protocolServer.addStateTransitionListener( new StateTransitionLogger( me.toString(), LoggerFactory.getLogger( StateTransitionLogger.class ) ) );
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

        // Timeout timer - triggers every 10 ms
        life.add( new Lifecycle()
        {
            private ScheduledExecutorService scheduler;

            @Override
            public void init()
                throws Throwable
            {
            }

            @Override
            public void start()
                throws Throwable
            {
                scheduler = Executors.newSingleThreadScheduledExecutor( new DaemonThreadFactory( "timeout" ) );

                scheduler.scheduleWithFixedDelay( new Runnable()
                {
                    long previousTime = System.currentTimeMillis();

                    @Override
                    public void run()
                    {
                        long now = System.currentTimeMillis();
                        long time = now - previousTime;
                        previousTime = now;

                        protocolServer.getTimeouts().tick( time );
                    }
                }, 0, 10, TimeUnit.MILLISECONDS );
            }

            @Override
            public void stop()
                throws Throwable
            {
                scheduler.shutdownNow();
            }

            @Override
            public void shutdown()
                throws Throwable
            {
            }
        } );
        
        return protocolServer;
    }
}
