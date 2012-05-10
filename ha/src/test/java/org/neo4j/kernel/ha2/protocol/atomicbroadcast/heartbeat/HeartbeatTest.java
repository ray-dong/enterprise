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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat;

import java.util.logging.Logger;

import org.junit.Test;
import org.neo4j.kernel.ha2.FixedNetworkLatencyStrategy;
import org.neo4j.kernel.ha2.HeartbeatServerFactory;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.TestProtocolServer;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;

/**
 * TODO
 */
public class HeartbeatTest
{
    Logger logger = Logger.getLogger( HeartbeatTest.class.getName() );

    private static final String ID1 = "1";
    private static final String ID2 = "2";
    private static final String ID3 = "3";
    private static final String[] POSSIBLE_SERVERS = new String[]{ID1,ID2,ID3};

    private NetworkMock network;

    private Heartbeat member1;
    private Heartbeat member2;
    private Heartbeat member3;

    @Test
    public void testNoFailures()
    {
        network = new NetworkMock(500, new HeartbeatServerFactory(), new FixedNetworkLatencyStrategy(), new FixedTimeoutStrategy( 1000 ));

        member1 = newMember( ID1, POSSIBLE_SERVERS );
        member2 = newMember( ID2, POSSIBLE_SERVERS );
        member3 = newMember( ID3, POSSIBLE_SERVERS );

        member1.addHeartbeatListener( new HeartbeatListener()
        {
            @Override
            public void failed( String server )
            {
                logger.severe( "Failed:" + server );
            }

            @Override
            public void alive( String server )
            {
                logger.severe( "Alive:" + server );
            }
        } );

        member1.join();
        member2.join();
        member3.join();

        network.tickUntilDone();

        logger.info( "SHOULD BE OK HERE" );

        network.tickUntilDone();
        network.tickUntilDone();
        network.tickUntilDone();

        member3.leave();

        network.tickUntilDone();
        logger.info( "3 SHOULD BE FAILED NOW" );

        member2.leave();
        member1.leave();

        network.tickUntilDone();
        logger.info( "1 & 2 SHOULD BE FAILED NOW" );
    }

    private Heartbeat newMember( String id, String... possibleServers )
    {
        TestProtocolServer server = network.addServer( id );
        Heartbeat member = server.newClient( Heartbeat.class );
        member.possibleServers( possibleServers );
        return member;
    }
}
