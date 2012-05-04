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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.LifeSupport;
import org.neo4j.kernel.ha2.MultiPaxosServer;
import org.neo4j.kernel.ha2.NetworkedServerFactory;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastMap;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class MultiPaxosNetworkTest
{
    @Test
    public void testBroadcast()
        throws ExecutionException, InterruptedException
    {
        LifeSupport life = new LifeSupport();
        NetworkedServerFactory serverFactory = new NetworkedServerFactory( life, StringLogger.SYSTEM );

        MultiPaxosServer server1 = serverFactory.newNetworkedServer( MapUtil.stringMap() );
        MultiPaxosServer server2 = serverFactory.newNetworkedServer( MapUtil.stringMap() );
        MultiPaxosServer server3 = serverFactory.newNetworkedServer( MapUtil.stringMap() );

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

        AtomicBroadcastListener broadcastListener = new AtomicBroadcastListener()
        {
            @Override
            public void learn( Object value )
            {
                System.out.println( value );
            }
        };

        atomicBroadcast1.addAtomicBroadcastListener( broadcastListener );
        atomicBroadcast2.addAtomicBroadcastListener( broadcastListener );
        atomicBroadcast3.addAtomicBroadcastListener( broadcastListener );

        AtomicBroadcastMap<String,String> map = new AtomicBroadcastMap<String,String>( atomicBroadcast1 );

        map.put( "foo", "bar" );
        String value = map.get( "foo");
        Assert.assertThat(value, CoreMatchers.equalTo( "bar" ));

        map.close();

        life.stop();
    }
}
