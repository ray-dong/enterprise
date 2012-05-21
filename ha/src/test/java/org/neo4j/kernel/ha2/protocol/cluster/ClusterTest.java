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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.kernel.ha2.FixedNetworkLatencyStrategy;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.PaxosClusterConfiguration;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TODO
 */
public class ClusterTest
{
    Logger logger = Logger.getAnonymousLogger();

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

        Assert.assertThat(config.get().getNodes(), CoreMatchers.equalTo( Arrays.asList(new URI(server1)) ));
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
        logger.info(config.get().toString());
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

        Cluster cluster2 = network.addServer(server2).newClient(Cluster.class);
        cluster2.join(new URI(server1));
        network.tickUntilDone();

        logger.severe( "2 JOINED!!!" );

        Cluster cluster3 = network.addServer(server3).newClient(Cluster.class);
        cluster3.join(new URI(server1));
        network.tickUntilDone();
        logger.severe( "3 JOINED!!!" );

        logger.info(config.get().toString());
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
