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

package org.neo4j.kernel.ha2.protocol.cluster;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;
import org.neo4j.kernel.ha2.FixedNetworkLatencyStrategy;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;

/**
 * TODO
 */
public class ClusterTest
{
    Logger logger = LoggerFactory.getLogger( ClusterTest.class );

    String server1 = "server1";
    String server2 = "server2";
    String server3 = "server3";

    @Test
    public void givenNotJoinedWhenCreateThenClusterWithOneIsCreated()
        throws ExecutionException, InterruptedException, URISyntaxException, TimeoutException
    {
        NetworkMock network = new NetworkMock(50,
                new MultiPaxosServerFactory(new ClusterConfiguration()),
                new FixedNetworkLatencyStrategy(0),
                new FixedTimeoutStrategy(1000));

        Cluster cluster = network.addServer(server1).newClient(Cluster.class);
        final AtomicReference<ClusterConfiguration> config = configListener( cluster );

        cluster.create();
        network.tickUntilDone();

        assertThat( config.get().getNodes(), hasItems( new URI( server1 ) ) );
    }

    @Test
    public void givenNotJoinedWhenJoinExistingClusterThenClusterWithTwoIsCreated() throws ExecutionException, InterruptedException, URISyntaxException, TimeoutException
    {
        NetworkMock network = new NetworkMock(50,
                new MultiPaxosServerFactory(new ClusterConfiguration()),
                new FixedNetworkLatencyStrategy(0),
                new FixedTimeoutStrategy(1000));

        Cluster cluster1 = network.addServer(server1).newClient( Cluster.class );

        cluster1.create();
        network.tickUntilDone();

        Cluster cluster2 = network.addServer(server2).newClient(Cluster.class);
        final AtomicReference<ClusterConfiguration> config = configListener( cluster2 );
        cluster2.join(new URI(server1));
        network.tickUntilDone();
        logger.info( config.get().toString() );
        assertThat( config.get().getNodes(), hasItems( new URI( server1 ), new URI( server2 ) ) );
    }

    @Test
    public void threeNodesCanCreateAndJoinIntoCluster() throws ExecutionException, InterruptedException, URISyntaxException, TimeoutException
    {
        NetworkMock network = new NetworkMock(50,
                new MultiPaxosServerFactory(new ClusterConfiguration()),
                new FixedNetworkLatencyStrategy(0),
                new FixedTimeoutStrategy(1000));

        Cluster cluster1 = network.addServer(server1).newClient( Cluster.class );
        final AtomicReference<ClusterConfiguration> config = configListener( cluster1 );

        cluster1.create();
        network.tickUntilDone();
        assertThat( config.get().getNodes(), equalTo( asList( new URI( server1 ) ) ) );

        Cluster cluster2 = network.addServer(server2).newClient(Cluster.class);
        final AtomicReference<ClusterConfiguration> config2 = configListener( cluster2 );

        cluster2.join(new URI(server1));
        network.tickUntilDone();
        assertThat( config.get().getNodes(), equalTo( asList( new URI( server1 ), new URI( server2 ) ) ) );
        assertThat( config2.get().getNodes(), equalTo( asList( new URI( server1 ), new URI( server2 ) ) ) );

        logger.info( "2 JOINED!!!" );

        Cluster cluster3 = network.addServer(server3).newClient( Cluster.class );

        final AtomicReference<ClusterConfiguration> config3 = configListener( cluster3 );

        cluster3.join( new URI( server2 ) );
        network.tickUntilDone();
        logger.info( "3 JOINED!!!" );

        logger.info( config.get().toString() );
        assertThat( config.get().getNodes(), equalTo( asList( new URI( server1 ), new URI( server2 ), new URI( server3 ) ) ));
        assertThat( config2.get().getNodes(), equalTo( asList( new URI( server1 ), new URI( server2 ), new URI( server3 ) ) ));
        assertThat( config3.get().getNodes(), equalTo( asList( new URI( server1 ), new URI( server2 ), new URI( server3 ) ) ));
    }

    @Test
    public void givenThreeNodeClusterWhenNodesLeaveThenClusterDisappears()
        throws URISyntaxException
    {
        NetworkMock network = new NetworkMock(50,
                new MultiPaxosServerFactory(new ClusterConfiguration()),
                new FixedNetworkLatencyStrategy(0),
                new FixedTimeoutStrategy(1000));

        Cluster cluster1 = network.addServer(server1).newClient( Cluster.class );
        final AtomicReference<ClusterConfiguration> config = configListener( cluster1 );
        cluster1.create();
        network.tickUntilDone();
        Cluster cluster2 = network.addServer(server2).newClient(Cluster.class);
        final AtomicReference<ClusterConfiguration> config2 = configListener( cluster2 );
        cluster2.join(new URI(server1));
        network.tickUntilDone();
        Cluster cluster3 = network.addServer(server3).newClient( Cluster.class );
        final AtomicReference<ClusterConfiguration> config3 = configListener( cluster3 );
        cluster3.join( new URI( server2 ) );
        network.tickUntilDone();

        logger.info( "3 LEAVING!!!" );
        cluster3.leave();
        network.tickUntilDone();

        assertThat( config.get().getNodes(), equalTo( asList( new URI( server1 ), new URI( server2 ) ) ) );
        assertThat( config2.get().getNodes(), equalTo( asList( new URI( server1 ), new URI( server2 ) ) ) );
        assertThat( config3.get().getNodes(), equalTo( Arrays.<URI>asList() ) );

        logger.info( "2 LEAVING!!!" );
        cluster2.leave();
        network.tickUntilDone();

        assertThat( config.get().getNodes(), equalTo( asList( new URI( server1 ) ) ) );
        assertThat( config2.get().getNodes(), equalTo( Arrays.<URI>asList(  ) ) );

        logger.info( "1 LEAVING!!!" );
        cluster1.leave();
        network.tickUntilDone();

        assertThat( config.get().getNodes(), equalTo( Arrays.<URI>asList(  ) ) );
    }

    private AtomicReference<ClusterConfiguration> configListener( Cluster cluster )
    {
        final AtomicReference<ClusterConfiguration> config = new AtomicReference<ClusterConfiguration>(  );
        cluster.addClusterListener(new ClusterListener()
        {
            @Override
            public void notifyClusterChange(ClusterConfiguration configuration)
            {
                config.set( configuration );
            }
        });
        return config;
    }
}
