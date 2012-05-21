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

import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.NetworkedServerFactory;
import org.neo4j.kernel.ha2.ProtocolServer;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastMap;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterConfiguration;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;

/**
 * TODO
 */
public class MultiPaxosNetworkTest
{
    @Test
    public void testBroadcast()
            throws ExecutionException, InterruptedException, URISyntaxException
    {
        LifeSupport life = new LifeSupport();
        NetworkedServerFactory serverFactory = new NetworkedServerFactory( life,
                new MultiPaxosServerFactory(new ClusterConfiguration("neo4j://localhost:5001","neo4j://localhost:5002","neo4j://localhost:5003")),
                new FixedTimeoutStrategy( 10000 ), StringLogger.SYSTEM );

        ProtocolServer server1 = serverFactory.newNetworkedServer( MapUtil.stringMap("port","5001") );
        ProtocolServer server2 = serverFactory.newNetworkedServer( MapUtil.stringMap("port","5002") );
        ProtocolServer server3 = serverFactory.newNetworkedServer( MapUtil.stringMap("port","5003") );

        life.start();

        AtomicBroadcast atomicBroadcast1 = server1.newClient( AtomicBroadcast.class );
        AtomicBroadcast atomicBroadcast2 = server2.newClient( AtomicBroadcast.class );
        AtomicBroadcast atomicBroadcast3 = server3.newClient( AtomicBroadcast.class );

        AtomicBroadcastMap<String,String> map = new AtomicBroadcastMap<String,String>( atomicBroadcast1 );
        AtomicBroadcastMap<String,String> map2 = new AtomicBroadcastMap<String,String>( atomicBroadcast2 );

        for (int i = 0; i < 10; i++ )
        {
            map.put( "foo" + i, "bar" + i );
        }

        System.out.println( "Set all values" );

        String value = map.get( "foo1");
        System.out.println( "Read value" );
        Assert.assertThat(value, CoreMatchers.equalTo( "bar1" ));

        System.out.println( "Read value:"+map2.get("foo1") );

        map.close();

        life.stop();
    }
}
