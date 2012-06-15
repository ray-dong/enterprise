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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.com_2.NetworkNodeTCP;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha2.BindingListener;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.NetworkedServerFactory;
import org.neo4j.kernel.ha2.ProtocolServer;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastMap;
import org.neo4j.kernel.ha2.protocol.cluster.Cluster;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterAdapter;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterConfiguration;
import org.neo4j.kernel.ha2.protocol.snapshot.Snapshot;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.slf4j.LoggerFactory;

import static org.neo4j.com_2.NetworkNodeTCP.Configuration.*;

/**
 * TODO
 */
public class MultiPaxosNetworkTest
{
    @Test
    public void testBroadcast()
        throws ExecutionException, InterruptedException, URISyntaxException, BrokenBarrierException
    {
        final LifeSupport life = new LifeSupport();
        NetworkedServerFactory serverFactory = new NetworkedServerFactory( life,
                new MultiPaxosServerFactory(new ClusterConfiguration("default", "neo4j://localhost:5001","neo4j://localhost:5002","neo4j://localhost:5003"), new InMemoryAcceptorInstanceStore()),
                new FixedTimeoutStrategy( 5000 ), LoggerFactory.getLogger( NetworkNodeTCP.class ) );

        final ProtocolServer server1 = serverFactory.newNetworkedServer( new ConfigurationDefaults(NetworkNodeTCP.Configuration.class).apply(MapUtil.stringMap( cluster_port.name(),"5001") ));
        final ProtocolServer server2 = serverFactory.newNetworkedServer( new ConfigurationDefaults(NetworkNodeTCP.Configuration.class).apply( MapUtil
                                                                                                                                                  .stringMap( cluster_port
                                                                                                                                                                  .name(), "5002" ) ) );
        final ProtocolServer server3 = serverFactory.newNetworkedServer( new ConfigurationDefaults(NetworkNodeTCP.Configuration.class).apply( MapUtil
                                                                                                                                                  .stringMap( cluster_port
                                                                                                                                                                  .name(), "5003" ) ) );

        server1.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                server1.newClient( Cluster.class ).create( "default" );
            }
        } );

        server2.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                server2.newClient( Cluster.class ).join( server1.getServerId() );
            }
        } );

        AtomicBroadcast atomicBroadcast1 = server1.newClient( AtomicBroadcast.class );
        AtomicBroadcast atomicBroadcast2 = server2.newClient( AtomicBroadcast.class );
        AtomicBroadcast atomicBroadcast3 = server3.newClient( AtomicBroadcast.class );
        Snapshot snapshot1 = server1.newClient( Snapshot.class );
        Snapshot snapshot2 = server2.newClient( Snapshot.class );
        Snapshot snapshot3 = server3.newClient( Snapshot.class );

        final AtomicBroadcastMap<String,String> map = new AtomicBroadcastMap<String,String>( atomicBroadcast1, snapshot1 );
        final AtomicBroadcastMap<String,String> map2 = new AtomicBroadcastMap<String,String>( atomicBroadcast2, snapshot2 );
        final AtomicBroadcastMap<String,String> map3 = new AtomicBroadcastMap<String,String>( atomicBroadcast3, snapshot3 );

        final Semaphore semaphore = new Semaphore(0 );

        server1.newClient( Cluster.class ).addClusterListener( new ClusterAdapter()
        {
            @Override
            public void joinedCluster( URI node )
            {
                LoggerFactory.getLogger(getClass()).info( "1 sees join by "+node );
            }
        } );

        server2.newClient( Cluster.class ).addClusterListener( new ClusterAdapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration nodes )
            {
                semaphore.release();
            }

            @Override
            public void joinedCluster( URI node )
            {
                LoggerFactory.getLogger(getClass()).info( "2 sees join by "+node );
            }
        } );

        life.start();

        semaphore.acquire();

        LoggerFactory.getLogger(getClass()).info( "Joined cluster - set data" );

        for (int i = 0; i < 100; i++ )
        {
            map.put( "foo" + i, "bar" + i );
        }

        LoggerFactory.getLogger(getClass()).info( "Set all values" );

        String value = map.get( "foo1");


        LoggerFactory.getLogger(getClass()).info( "3 joins 1" );
        server3.newClient( Cluster.class ).addClusterListener( new ClusterAdapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration nodes )
            {
                LoggerFactory.getLogger(getClass()).info( "3 entered cluster of:"+nodes.getNodes() );
                semaphore.release();
            }
        } );
        server3.newClient( Cluster.class ).join( server1.getServerId() );
        semaphore.acquire(  );

        LoggerFactory.getLogger(getClass()).info( "Read value1" );
        Assert.assertThat(value, CoreMatchers.equalTo( "bar1" ));

        map2.put( "foo2","666" );

        LoggerFactory.getLogger(getClass()).info( "Read value2:"+map2.get("foo1") );
        LoggerFactory.getLogger(getClass()).info( "Read value3:" + map2.get( "foo2" ) );

        LoggerFactory.getLogger(getClass()).info( "Read value4:" + map3.get( "foo1" ) );
        LoggerFactory.getLogger(getClass()).info( "Read value5:" + map3.get( "foo99" ) );
        Assert.assertThat(map3.get( "foo1" ), CoreMatchers.equalTo( "bar1" ));
        Assert.assertThat(map3.get( "foo99" ), CoreMatchers.equalTo( "bar99" ));

        map.close();
        map2.close();
        map3.close();

        life.stop();

    }
}
