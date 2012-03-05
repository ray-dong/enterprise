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

import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.initial;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.master;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.slave;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.start;

import org.junit.Test;
import org.neo4j.kernel.ha2.StateTransitionExpectations;
import org.neo4j.kernel.ha2.protocol.RingParticipant;

/**
 * TODO
 */
public class TokenRingProtocolTest
{
    @Test
    public void startNewRingWith3ParticipantsAndShutItDown()
    {
        NetworkMock tokenRing = new NetworkMock();
        StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();

        RingParticipant server1 = new RingParticipant("server1");
        tokenRing.addParticipant(server1, expectations.newExpectations().includeUnchangedStates()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.failure, master )
                .expect( TokenRingMessage.discoverRing, master )
                .expect( TokenRingMessage.discoverRing, master )
                .expect( TokenRingMessage.leaveRing, start )
                .build( server1 ) );

        RingParticipant server2 = new RingParticipant("server2");
        tokenRing.addParticipant(server2, expectations.newExpectations().includeUnchangedStates()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.newAfter, initial )
                .expect( TokenRingMessage.ringDiscovered, slave )
                .expect( TokenRingMessage.discoverRing, slave )
                .expect( TokenRingMessage.becomeMaster, master )
                .expect( TokenRingMessage.leaveRing, start )
                .build( server2 ) );

        RingParticipant server3 = new RingParticipant("server3");
        tokenRing.addParticipant(server3, expectations.newExpectations().includeUnchangedStates()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.newAfter, initial )
                .expect( TokenRingMessage.ringDiscovered, slave )
                .expect( TokenRingMessage.newAfter, slave )
                .expect( TokenRingMessage.newAfter, slave )
                .expect( TokenRingMessage.leaveRing, start )
                .expect( TokenRingMessage.newAfter, start )
                .build( server3 ) );

        tokenRing.removeParticipant(server1);
        tokenRing.removeParticipant(server2);
        tokenRing.removeParticipant(server3);
        
        expectations.verify();
    }

    @Test
    public void startNewRingWith3ParticipantsAndSlavesLeave()
    {
        NetworkMock tokenRing = new NetworkMock();
        StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();

        RingParticipant server1 = new RingParticipant("server1");
        tokenRing.addParticipant(server1, expectations.newExpectations().includeUnchangedStates()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.failure, master )
                .expect( TokenRingMessage.discoverRing, master )
                .expect( TokenRingMessage.discoverRing, master )
                .expect( TokenRingMessage.newBefore, master )
                .expect( TokenRingMessage.newBefore, master )
                .build( server1 ) );

        RingParticipant server2 = new RingParticipant("server2");
        tokenRing.addParticipant(server2, expectations.newExpectations().includeUnchangedStates()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.newAfter, initial )
                .expect( TokenRingMessage.ringDiscovered, slave )
                .expect( TokenRingMessage.discoverRing, slave )
                .expect( TokenRingMessage.leaveRing, start )
                .expect( TokenRingMessage.newAfter, start )
                .build( server2 ) );

        RingParticipant server3 = new RingParticipant("server3");
        tokenRing.addParticipant(server3, expectations.newExpectations().includeUnchangedStates()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.newAfter, initial )
                .expect( TokenRingMessage.ringDiscovered, slave )
                .expect( TokenRingMessage.leaveRing, start )
                .expect( TokenRingMessage.newAfter, start )
                .build( server3 ) );

        tokenRing.removeParticipant(server2);
        tokenRing.removeParticipant(server3);
        
        expectations.verify();
    }

    @Test
    public void startNewRingWith3ParticipantsAndSendTokenAround()
    {
        NetworkMock tokenRing = new NetworkMock();
        StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();

        RingParticipant participant1 = new RingParticipant("server1");
        NetworkMock.Server server1 = tokenRing.addParticipant(participant1, expectations.newExpectations()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.failure, master )
                .expect( TokenRingMessage.sendToken, slave )
                .expect( TokenRingMessage.becomeMaster, master )
                .build( participant1 ) );
        
        RingParticipant participant2 = new RingParticipant("server2");
        NetworkMock.Server server2 = tokenRing.addParticipant(participant2, expectations.newExpectations()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.ringDiscovered, slave )
                .expect( TokenRingMessage.becomeMaster, master )
                .expect( TokenRingMessage.sendToken, slave )
                .build( participant2 ) );
        
        RingParticipant participant3 = new RingParticipant("server3");
        NetworkMock.Server server3 = tokenRing.addParticipant(participant3, expectations.newExpectations()
                .expect( TokenRingMessage.start, initial )
                .expect( TokenRingMessage.ringDiscovered, slave )
                .build( participant3 ) );
        
        TokenRing tokenRing1 = server1.newProxy( TokenRing.class );
        tokenRing1.sendToken();
        server2.newProxy( TokenRing.class ).sendToken();

        expectations.verify();
    }
}
