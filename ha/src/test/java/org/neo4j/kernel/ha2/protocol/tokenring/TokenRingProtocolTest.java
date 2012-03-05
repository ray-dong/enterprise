/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.ha2.protocol.tokenring;

import org.junit.Test;
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

        RingParticipant server1 = new RingParticipant("server1");
        tokenRing.addParticipant(server1);

        RingParticipant server2 = new RingParticipant("server2");
        tokenRing.addParticipant(server2);

        RingParticipant server3 = new RingParticipant("server3");
        tokenRing.addParticipant(server3);

        tokenRing.removeParticipant(server1);
        tokenRing.removeParticipant(server2);
        tokenRing.removeParticipant(server3);
    }

    @Test
    public void startNewRingWith3ParticipantsAndSlavesLeave()
    {
        NetworkMock tokenRing = new NetworkMock();

        RingParticipant server1 = new RingParticipant("server1");
        tokenRing.addParticipant(server1);

        RingParticipant server2 = new RingParticipant("server2");
        tokenRing.addParticipant(server2);

        RingParticipant server3 = new RingParticipant("server3");
        tokenRing.addParticipant(server3);

        tokenRing.removeParticipant(server2);
        tokenRing.removeParticipant(server3);
    }

    @Test
    public void startNewRingWith3ParticipantsAndSendTokenAround()
    {
        NetworkMock tokenRing = new NetworkMock();

        NetworkMock.Server server1 = tokenRing.addParticipant(new RingParticipant("server1"));
        NetworkMock.Server server2 = tokenRing.addParticipant(new RingParticipant("server2"));
        NetworkMock.Server server3 = tokenRing.addParticipant(new RingParticipant("server3"));

        TokenRing tokenRing1 = server1.newProxy( TokenRing.class );
        tokenRing1.sendToken();
        server2.newProxy( TokenRing.class ).sendToken();
    }
}
