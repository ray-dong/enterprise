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

import java.util.concurrent.ExecutionException;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.NetworkedServerFactory;
import org.neo4j.kernel.ha2.ProtocolServer;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastMap;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;

/**
 * TODO
 */
public class MultiPaxosNetworkTest
{
//    @Test
    public void testBroadcast()
        throws ExecutionException, InterruptedException
    {
        LifeSupport life = new LifeSupport();
        NetworkedServerFactory serverFactory = new NetworkedServerFactory( life, new MultiPaxosServerFactory(), new FixedTimeoutStrategy( 1000 ), StringLogger.SYSTEM );

        ProtocolServer server1 = serverFactory.newNetworkedServer( MapUtil.stringMap() );
        ProtocolServer server2 = serverFactory.newNetworkedServer( MapUtil.stringMap() );
        ProtocolServer server3 = serverFactory.newNetworkedServer( MapUtil.stringMap() );

        life.start();

        AtomicBroadcast atomicBroadcast1 = server1.newClient( AtomicBroadcast.class );
        atomicBroadcast1.possibleServers( server1.getServerId(), server2.getServerId(), server3.getServerId() );
        atomicBroadcast1.join();

        AtomicBroadcast atomicBroadcast2 = server2.newClient( AtomicBroadcast.class );
        atomicBroadcast2.possibleServers( server1.getServerId(), server2.getServerId(), server3.getServerId() );
        atomicBroadcast2.join();

        AtomicBroadcast atomicBroadcast3 = server3.newClient( AtomicBroadcast.class );
        atomicBroadcast3.possibleServers( server1.getServerId(), server2.getServerId(), server3.getServerId() );
        atomicBroadcast3.join();

        AtomicBroadcastMap<String,String> map = new AtomicBroadcastMap<String,String>( atomicBroadcast1 );

        for (int i = 0; i < 10; i++ )
        {
            map.put( "foo" + i, "bar" + i );
        }

        System.out.println( "Set all values" );

        String value = map.get( "foo1");
        System.out.println( "Read value" );
        Assert.assertThat(value, CoreMatchers.equalTo( "bar1" ));

        map.close();

        life.stop();
    }
}
