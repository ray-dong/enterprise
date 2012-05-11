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

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.kernel.ha2.FixedNetworkLatencyStrategy;
import org.neo4j.kernel.ha2.MultiPaxosServerFactory;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.TestProtocolServer;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastMap;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;

/**
 * TODO
 */
public class MultiPaxosTest
{
    private static final String ID1 = "1";
    private static final String ID2 = "2";
    private static final String ID3 = "3";
    private static final String[] POSSIBLE_SERVERS = new String[]{ID1,ID2,ID3};

    private NetworkMock network;

    private AtomicBroadcast member1;
    private AtomicBroadcast member2;
    private AtomicBroadcast member3;

    @Test
    public void testDecision()
        throws ExecutionException, InterruptedException
    {
        network = new NetworkMock(50, new MultiPaxosServerFactory(), new FixedNetworkLatencyStrategy(0), new FixedTimeoutStrategy( 1000 ));

        member1 = newMember( ID1, POSSIBLE_SERVERS );
        member2 = newMember( ID2, POSSIBLE_SERVERS );
        member3 = newMember( ID3, POSSIBLE_SERVERS );

        Map<String, String> map1 = new AtomicBroadcastMap<String,String>(member1);
        Map<String, String> map2 = new AtomicBroadcastMap<String,String>(member2);

        map1.put("foo", "bar");
        network.tickUntilDone();
        Object foo = map1.get( "foo" );
        Assert.assertThat( foo.toString(), equalTo( "bar" ) );

        map1.put( "bar", "foo" );
        network.tickUntilDone();
        Object bar = map2.get( "bar" );
        Assert.assertThat( bar.toString(), equalTo( "foo" ) );

        map1.put( "foo", "bar2" );
        network.tickUntilDone();
        foo = map2.get( "foo" );
        Assert.assertThat( foo.toString(), equalTo( "bar2" ) );

        map1.clear();
        network.tickUntilDone();
        foo = map2.get( "foo" );
        Assert.assertThat( foo , CoreMatchers.nullValue() );
    }

    private AtomicBroadcast newMember( String id, String... possibleServers )
    {
        TestProtocolServer server = network.addServer( id );
        AtomicBroadcast member = server.newClient( AtomicBroadcast.class );
        member.possibleServers( possibleServers );
        member.join();
        return member;
    }
}
