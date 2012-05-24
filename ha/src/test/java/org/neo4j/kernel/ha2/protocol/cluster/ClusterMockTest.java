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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.FixedNetworkLatencyStrategy;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;
import org.neo4j.test.LoggerRule;

import static org.junit.Assert.*;

/**
 * TODO
 */
@RunWith(value = Parameterized.class)
public class ClusterMockTest
{
    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][]
                             {
                                 {
                                     // 3 nodes join and then leaves
                                     3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                                         join( 100L,1 ).
                                         join( 100L,2 ).
                                         join( 100L,3 ).
                                         leave( 100L, 3 ).
                                         leave( 100L, 2 ).
                                         leave( 100L, 1 )
                                 },
                                 {
                                     // 7 nodes join and then leaves
                                     7, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                                         join( 100L,1 ).
                                         join( 100L,2 ).
                                         join( 100L,3 ).
                                         join( 100L,4 ).
                                         join( 100L,5 ).
                                         join( 100L,6 ).
                                         join( 100L, 7 ).
                                         leave( 100L, 7 ).
                                         leave( 100L, 6 ).
                                         leave( 100L, 5 ).
                                         leave( 100L, 4 ).
                                         leave( 100L, 3 ).
                                         leave( 100L, 2 ).
                                         leave( 100L, 1 )
                                 },
                                 {
                                     // 1 node join, then 3 nodes try to join at roughly the same time
                                     4, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                                         join( 100L,1 ).
                                         join( 100L,2 ).
                                         join( 10L,3 ).
/*
                                         join( 10L,"server4" ).
                                         leave( 500L, "server4" ).
*/
                                         leave( 500L, 3 ).
                                         leave( 100L, 2 ).
                                         leave( 100L, 1 )
                                 },
                                 {
                                     // 2 nodes join, and then one leaves as the third joins
                                     3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                                         join( 100L,1 ).
                                         join( 100L,2 ).
                                         leave( 90L, 2 ).
                                         join( 20L, 3)
                                 },
                                 {
                                     3, DEFAULT_NETWORK(), new ClusterTestScriptRandom( 1337830212532839000L )
                                 }
                             });
    }

    public static NetworkMock DEFAULT_NETWORK(){ return new NetworkMock( 10, new MultiPaxosServerFactory(new ClusterConfiguration()),
                                            new FixedNetworkLatencyStrategy(10),
                                            new FixedTimeoutStrategy(1000) );}

    static List<Cluster> servers = new ArrayList<Cluster>(  );
    static List<Cluster> out = new ArrayList<Cluster>( );
    static List<Cluster> in = new ArrayList<Cluster>();

    @Rule
    public static LoggerRule logger = new LoggerRule();

    public static NetworkMock network;

    List<AtomicReference<ClusterConfiguration>> configurations = new ArrayList<AtomicReference<ClusterConfiguration>>(  );

    ClusterTestScript script;

    public ClusterMockTest( int nrOfServers, NetworkMock mock, ClusterTestScript script )
        throws URISyntaxException
    {
        this.script = script;

        network = mock;
        out.clear();
        in.clear();

        for (int i = 0; i < nrOfServers; i++)
        {
            final URI uri = new URI( "server"+(i+1) );
            final Cluster cluster2 = network.addServer(uri.toString()).newClient(Cluster.class);
            final AtomicReference<ClusterConfiguration> config2 = clusterStateListener( uri, cluster2 );

            servers.add( cluster2 );
            out.add( cluster2 );
            configurations.add( config2 );
        }
    }

    @Test
    public void testCluster()
        throws ExecutionException, InterruptedException, URISyntaxException, TimeoutException
    {
        for (int i = 0; i < 1000; i++)
        {
            logger.getLogger().info( "Round " + i +", time:"+network.getTime());

            script.tick( network.getTime() );

            if( i % 100 == 0 )
            {
                network.tickUntilDone();
                verifyConfigurations();
                assertEquals( "In:" + in + ", Out:" + out, network.getServers().size(), Iterables.count( Iterables.<Cluster, List<Cluster>>flatten( in, out ) ) );
            }
            else
            {
                network.tick();
            }
        }

        // Let messages settle
        network.tickUntilDone();
        verifyConfigurations();

        logger.getLogger().info( "All nodes leave" );

        // All leave
        for( Cluster cluster : new ArrayList<Cluster>(in) )
        {
            logger.getLogger().info( "Leaving:"+cluster );
            cluster.leave();
            network.tickUntilDone();
        }

        verifyConfigurations();
    }

    private AtomicReference<ClusterConfiguration> clusterStateListener( final URI uri, final Cluster cluster )
    {
        final AtomicReference<ClusterConfiguration> config = new AtomicReference<ClusterConfiguration>(  );
        cluster.addClusterListener( new ClusterListener()
        {
            @Override
            public void enteredCluster( Iterable<URI> nodes )
            {
                logger.getLogger().info( uri + " entered cluster:" + nodes );
                config.set( new ClusterConfiguration() );
                config.get().setNodes( nodes );
                in.add( cluster );
            }

            @Override
            public void joinedCluster( URI node )
            {
                logger.getLogger().info( uri + " sees a join:" + node.toString() );
                config.get().joined( node );
            }

            @Override
            public void leftCluster( URI node )
            {
                logger.getLogger().info( uri + " sees a leave:" + node.toString() );
                config.get().left( node );
            }

            @Override
            public void leftCluster()
            {
                out.add( cluster );
                config.set( null );
            }
        } );
        return config;
    }

    private void verifyConfigurations()
    {
        List<URI> nodes = null;
        for( int j = 0; j < configurations.size(); j++ )
        {
            AtomicReference<ClusterConfiguration> configurationAtomicReference = configurations.get( j );
            if (configurationAtomicReference.get() != null)
            {
                if (nodes == null)
                    nodes = configurationAtomicReference.get().getNodes();
                else
                    assertEquals( "Config for server" + (j+1) + " is wrong", nodes, configurationAtomicReference.get().getNodes() );

            }
        }
    }

    public interface ClusterTestScript
    {
        void tick(long time);
    }

    public static class ClusterTestScriptDSL
        implements ClusterTestScript
    {
        public abstract static class ClusterAction
            implements Runnable
        {
            public long time;
        }

        private Queue<ClusterAction> actions = new LinkedList<ClusterAction>();

        private long now = 0;

        public ClusterTestScriptDSL join(long time, final int joinServer)
        {
            ClusterAction joinAction = new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster joinCluster = servers.get( joinServer-1 );
                    for( Cluster cluster : out )
                    {
                        if (cluster.equals( joinCluster ))
                        {
                            out.remove( cluster );
                            logger.getLogger().info( "Join:"+cluster.toString() );
                            if (in.isEmpty())
                            {
                                cluster.create();
                            } else
                            {
                                try
                                {
                                    cluster.join( new URI( in.get( 0 ).toString()) );
                                }
                                catch( URISyntaxException e )
                                {
                                    e.printStackTrace();
                                }
                            }
                            break;
                        }
                    }
                }
            };
            joinAction.time = now+time;
            actions.offer( joinAction );
            now += time;
            return this;
        }

        public ClusterTestScriptDSL leave(long time, final int leaveServer)
        {
            ClusterAction leaveAction = new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster leaveCluster = servers.get( leaveServer-1 );
                    for( Cluster cluster : in )
                    {
                        if (cluster.equals( leaveCluster ))
                        {
                            in.remove( cluster );
                            cluster.leave();
                            logger.getLogger().info( "Leave:" + cluster.toString() );
                            break;
                        }
                    }
                }
            };
            leaveAction.time = now+time;
            actions.offer( leaveAction );
            now += time;
            return this;
        }

        @Override
        public void tick( long time )
        {
            while (!actions.isEmpty() && actions.peek().time == time)
            {
                actions.poll().run();
            }
        }
    }

    public static class ClusterTestScriptRandom
        implements ClusterTestScript
    {
        private final long seed;
        private final Random random;

        public ClusterTestScriptRandom(long seed)
        {
            if (seed == -1)
                seed = System.nanoTime();
            this.seed = seed;
            random = new Random( seed );
        }

        @Override
        public void tick( long time )
        {
            if (time == 0)
                logger.getLogger().info( "Random seed:"+seed );

            if (random.nextDouble() >= 0.9)
            {
                if (random.nextDouble() > 0.5  && !out.isEmpty())
                {
                    int idx = random.nextInt( out.size() );
                    Cluster cluster = out.remove( idx );

                    if (in.isEmpty())
                    {
                        cluster.create();
                    } else
                    {
                        try
                        {
                            cluster.join( new URI( in.get( 0 ).toString()) );
                        }
                        catch( URISyntaxException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    logger.getLogger().info( "Enter cluster:"+cluster.toString() );

                } else if (!in.isEmpty())
                {
                    int idx = random.nextInt( in.size() );
                    Cluster cluster = in.remove( idx );
                    cluster.leave( );
                    logger.getLogger().info( "Leave cluster:" + cluster.toString() );
                }
            }
        }
    }
}
