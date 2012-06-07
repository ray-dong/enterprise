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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.FixedNetworkLatencyStrategy;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.MultipleFailureLatencyStrategy;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.ScriptableNetworkFailureLatencyStrategy;
import org.neo4j.kernel.ha2.TestProtocolServer;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastListenerDeserializer;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.Payload;
import org.neo4j.kernel.ha2.protocol.heartbeat.Heartbeat;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatListener;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;
import org.neo4j.kernel.ha2.timeout.MessageTimeoutStrategy;
import org.neo4j.test.LoggerRule;

import static org.junit.Assert.*;

/**
 * Base class for cluster tests
 */
public class ClusterMockTest
{
    public static NetworkMock DEFAULT_NETWORK()
    {
        return new NetworkMock( 10, new MultiPaxosServerFactory(new ClusterConfiguration("default")),
                                            new MultipleFailureLatencyStrategy( new FixedNetworkLatencyStrategy(10), new ScriptableNetworkFailureLatencyStrategy()),
                                            new MessageTimeoutStrategy(new FixedTimeoutStrategy(500) )
                                                .timeout( HeartbeatMessage.send_heartbeat, 200 ));
    }

    List<TestProtocolServer> servers = new ArrayList<TestProtocolServer>(  );
    List<Cluster> out = new ArrayList<Cluster>( );
    List<Cluster> in = new ArrayList<Cluster>();
    List<AtomicReference<ClusterConfiguration>> configurations = new ArrayList<AtomicReference<ClusterConfiguration>>(  );

    @Rule
    public LoggerRule logger = new LoggerRule();

    public NetworkMock network;

    ClusterTestScript script;

    ExecutorService executor;

    @Before
    public void setup()
    {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown()
    {
        executor.shutdownNow();
    }

    protected void testCluster(int nrOfServers, NetworkMock mock, ClusterTestScript script)
        throws ExecutionException, InterruptedException, URISyntaxException, TimeoutException
    {
        this.script = script;

        network = mock;
        servers.clear();
        out.clear();
        in.clear();
        configurations.clear();

        for (int i = 0; i < nrOfServers; i++)
        {
            final URI uri = new URI( "server"+(i+1) );
            TestProtocolServer server = network.addServer( uri.toString() );
            final Cluster cluster = server.newClient( Cluster.class );
            final AtomicReference<ClusterConfiguration> config2 = clusterStateListener( uri, cluster );

            server.newClient( Heartbeat.class ).addHeartbeatListener( new HeartbeatListener()
                    {
                        @Override
                        public void failed( URI server )
                        {
                            logger.getLogger().warn( uri+": Failed:" + server );
                        }

                        @Override
                        public void alive( URI server )
                        {
                            logger.getLogger().info( uri+": Alive:" + server );
                        }
                    } );
            server.newClient( AtomicBroadcast.class ).addAtomicBroadcastListener( new AtomicBroadcastListenerDeserializer( new AtomicBroadcastSerializer(), new AtomicBroadcastListener()
            {
                @Override
                public void receive( Object value )
                {
                    logger.getLogger().info( uri+" received: "+value );
                }
            } ));

            servers.add( server );
            out.add( cluster );
            configurations.add( config2 );
        }

        // Run test
        for (int i = 0; i < script.rounds(); i++)
        {
            logger.getLogger().info( "Round " + i +", time:"+network.getTime());

            script.tick( network.getTime() );

            network.tick();
        }

        // Let messages settle
        network.tick( 100 );
        verifyConfigurations();

        logger.getLogger().info( "All nodes leave" );

        // All leave
        for( Cluster cluster : new ArrayList<Cluster>(in) )
        {
            logger.getLogger().info( "Leaving:"+cluster );
            cluster.leave();
            in.remove( cluster );
            network.tick( 10 );
        }

        verifyConfigurations();
    }

    private AtomicReference<ClusterConfiguration> clusterStateListener( final URI uri, final Cluster cluster )
    {
        final AtomicReference<ClusterConfiguration> config = new AtomicReference<ClusterConfiguration>(  );
        cluster.addClusterListener( new ClusterListener()
        {
            @Override
            public void enteredCluster( ClusterConfiguration configuration )
            {
                logger.getLogger().info( uri + " entered cluster:" + configuration.getNodes() );
                config.set( new ClusterConfiguration( configuration ) );
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
                logger.getLogger().info( uri + " left cluster" );
                out.add( cluster );
                config.set( null );
            }
        } );
        return config;
    }

    public void verifyConfigurations()
    {
        logger.getLogger().info( "Verify configurations" );
        List<URI> nodes = null;
        int foundConfiguration = 0;
        for( int j = 0; j < configurations.size(); j++ )
        {
            AtomicReference<ClusterConfiguration> configurationAtomicReference = configurations.get( j );
            if (configurationAtomicReference.get() != null)
            {
                logger.getLogger().info( "   Server "+(j+1)+": "+configurationAtomicReference.get().getNodes() );
                foundConfiguration++;
                if( nodes == null )
                {
                    nodes = configurationAtomicReference.get().getNodes();
                }
                else
                {
                    assertEquals( "Config for server" + ( j + 1 ) + " is wrong", nodes, configurationAtomicReference.get()
                        .getNodes() );
                }

            }
        }

        if( foundConfiguration > 0 )
        {
            assertEquals( "Nr of found active nodes does not match configuration size", nodes.size(), foundConfiguration );
        }

        assertEquals( "In:" + in + ", Out:" + out, network.getServers().size(), Iterables.count( Iterables.<Cluster, List<Cluster>>flatten( in, out ) ) );
    }

    public interface ClusterTestScript
    {
        int rounds();
        void tick(long time);
    }

    public class ClusterTestScriptDSL
        implements ClusterTestScript
    {
        public abstract class ClusterAction
            implements Runnable
        {
            public long time;
        }

        private Queue<ClusterAction> actions = new LinkedList<ClusterAction>();
        private AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer();

        private int rounds = 100;
        private long now = 0;

        public ClusterTestScriptDSL rounds(int n)
        {
            rounds = n;
            return this;
        }

        public ClusterTestScriptDSL join(int time, final int joinServer)
        {
            return addAction(new ClusterAction()
                        {
                            @Override
                            public void run()
                            {
                                Cluster joinCluster = servers.get( joinServer-1 ).newClient( Cluster.class );
                                for( final Cluster cluster : out )
                                {
                                    if (cluster.equals( joinCluster ))
                                    {
                                        out.remove( cluster );
                                        logger.getLogger().info( "Join:"+cluster.toString() );
                                        if (in.isEmpty())
                                        {
                                            cluster.create("default");
                                        } else
                                        {
                                            try
                                            {
                                                final Future<ClusterConfiguration> result = cluster.join( new URI( in.get( 0 ).toString()) );
                                                executor.submit( new Runnable()
                                                {
                                                    @Override
                                                    public void run()
                                                    {
                                                        try
                                                        {
                                                            ClusterConfiguration clusterConfiguration = result.get();
                                                            logger.getLogger().info( "**** Cluster configuration:"+ clusterConfiguration );
                                                        }
                                                        catch( Exception e )
                                                        {
                                                            logger.getLogger().info( "**** Node could not join cluster:"+ e.getMessage() );
                                                            out.add( cluster );
                                                        }
                                                    }
                                                });
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
                        }, time);
        }

        public ClusterTestScriptDSL leave(long time, final int leaveServer)
        {
            return addAction( new ClusterAction()
                        {
                            @Override
                            public void run()
                            {
                                Cluster leaveCluster = servers.get( leaveServer-1 ).newClient( Cluster.class );
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
                        }, time );
        }

        public ClusterTestScriptDSL down( int time, final int serverDown )
        {
            return addAction( new ClusterAction()
                        {
                            @Override
                            public void run()
                            {
                                Cluster server = servers.get( serverDown-1 ).newClient( Cluster.class );
                                network.getNetworkLatencyStrategy().getStrategy( ScriptableNetworkFailureLatencyStrategy.class ).nodeIsDown( server.toString() );
                                logger.getLogger().info( server+ " is down" );
                            }
                        }, time );
        }

        public ClusterTestScriptDSL up( int time, final int serverUp )
        {
            return addAction( new ClusterAction()
                        {
                            @Override
                            public void run()
                            {
                                Cluster server = servers.get( serverUp-1 ).newClient( Cluster.class );
                                network.getNetworkLatencyStrategy().getStrategy( ScriptableNetworkFailureLatencyStrategy.class ).nodeIsUp( server
                                                                                                                                               .toString() );
                                logger.getLogger().info( server+ " is up" );
                            }
                        }, time );
        }

        public ClusterTestScriptDSL broadcast( int time, final int server, final Object value )
        {
            return addAction( new ClusterAction()
                        {
                            @Override
                            public void run()
                            {
                                AtomicBroadcast broadcast = servers.get( server-1 ).newClient( AtomicBroadcast.class );
                                try
                                {
                                    broadcast.broadcast( serializer.broadcast( value ) );
                                }
                                catch( IOException e )
                                {
                                    e.printStackTrace();
                                }
                            }
                        }, time );
        }

        public ClusterTestScriptDSL message( int time, final String msg )
        {
            return addAction( new ClusterAction()
                        {
                            @Override
                            public void run()
                            {
                                logger.getLogger().info( msg );
                            }
                        }, time );
        }

        public ClusterTestScriptDSL verifyConfigurations( long time )
        {
            return addAction( new ClusterAction()
                        {
                            @Override
                            public void run()
                            {
                                ClusterMockTest.this.verifyConfigurations();
                            }
                        }, time );
        }

        private ClusterTestScriptDSL addAction( ClusterAction action, long time )
        {
            action.time = now+time;
            actions.offer( action );
            now += time;
            return this;
        }

        @Override
        public int rounds()
        {
            return rounds;
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

    public class ClusterTestScriptRandom
        implements ClusterTestScript
    {
        private final long seed;
        private final Random random;

        public ClusterTestScriptRandom(long seed)
        {
            if( seed == -1 )
            {
                seed = System.nanoTime();
            }
            this.seed = seed;
            random = new Random( seed );
        }

        @Override
        public int rounds()
        {
            return 100;
        }

        @Override
        public void tick( long time )
        {
            if( time == 0 )
            {
                logger.getLogger().info( "Random seed:" + seed+"L" );
            }

            if (random.nextDouble() >= 0.8)
            {
                double inOrOut = (in.size()-out.size())/((double)servers.size());
                double whatToDo = random.nextDouble() + inOrOut;
                logger.getLogger().info( "What to do:"+whatToDo );

                if (whatToDo < 0.5  && !out.isEmpty())
                {
                    int idx = random.nextInt( out.size() );
                    Cluster cluster = out.remove( idx );

                    if (in.isEmpty())
                    {
                        cluster.create("default");
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
