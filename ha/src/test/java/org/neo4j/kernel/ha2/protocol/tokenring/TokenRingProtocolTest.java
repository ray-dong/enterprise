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

package org.neo4j.kernel.ha2.protocol.tokenring;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.StateTransitionExpectations;
import org.neo4j.kernel.ha2.TestServer;

import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.*;

/**
 * TODO
 */
public class TokenRingProtocolTest
{
    @Before
    public void setupLogging()
    {
        for( Handler handler : Logger.getLogger( "" ).getHandlers() )
        {
            Logger.getLogger( "" ).removeHandler( handler );
        }

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter( new Formatter()
        {
            @Override
            public String format( LogRecord record )
            {
                return record.getMessage()+"\n";
            }
        });

        Logger.getLogger( "" ).addHandler( handler );
    }
    
    @Test
    public void startNewRingWith3ParticipantsAndShutItDown()
    {
        NetworkMock network = new NetworkMock();
        StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();

        String server1 = "server1";
        network.addServer( server1, expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.failure, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.leaveRing, start )
            .build( server1 ) );

        network.tickUntilDone();

        String server2 = "server2";
        network.addServer( server2, expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.newAfter, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.discoverRing, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .expect( TokenRingMessage.leaveRing, start )
            .build( server2 ) );

        network.tickUntilDone();

        String server3 = "server3";
        network.addServer( server3, expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.newAfter, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.newAfter, slave )
            .expect( TokenRingMessage.newAfter, slave )
            .expect( TokenRingMessage.leaveRing, start )
            .expect( TokenRingMessage.newAfter, start )
            .build( server3 ) );

        network.tickUntilDone();

        network.removeServer( server1 );

        network.tickUntilDone();

        network.removeServer( server2 );

        network.tickUntilDone();

        network.removeServer( server3 );

        network.tickUntilDone();

        expectations.verify();
    }

    @Test
    public void startNewRingWith3ParticipantsAndSlavesLeave()
    {
        NetworkMock network = new NetworkMock();
        StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();

        String server1 = "server1";
        network.addServer( server1, expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.failure, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.newBefore, master )
            .expect( TokenRingMessage.newBefore, master )
            .build( server1 ) );

        String server2 = "server2";
        network.addServer( server2, expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.newAfter, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.discoverRing, slave )
            .expect( TokenRingMessage.leaveRing, start )
            .expect( TokenRingMessage.newAfter, start )
            .build( server2 ) );

        String server3 = "server3";
        network.addServer( server3, expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.newAfter, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.leaveRing, start )
            .expect( TokenRingMessage.newAfter, start )
            .build( server3 ) );

        network.tickUntilDone();

        network.removeServer( server2 );
        network.removeServer( server3 );
        
        network.tickUntilDone();

        expectations.verify();
    }

    @Test
    public void startNewRingWith3ParticipantsAndSendTokenAround()
    {
        NetworkMock network = new NetworkMock();
        StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();

        String participant1 = "server1";
        TestServer server1 = network.addServer( participant1, expectations.newExpectations()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.failure, master )
            .expect( TokenRingMessage.sendToken, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .build( participant1 ) );
        
        String participant2 = "server2";
        TestServer server2 = network.addServer( participant2, expectations.newExpectations()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .expect( TokenRingMessage.sendToken, slave )
            .build( participant2 ) );
        
        String participant3 = "server3";
        TestServer server3 = network.addServer( participant3, expectations.newExpectations()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .build( participant3 ) );
        
        network.tickUntilDone();

        TokenRing tokenRing1 = server1.newClient( TokenRing.class );
        tokenRing1.sendToken();

        network.tickUntilDone();

        expectations.verify();
    }
}
