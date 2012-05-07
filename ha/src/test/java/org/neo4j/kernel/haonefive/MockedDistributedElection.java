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
package org.neo4j.kernel.haonefive;

import static org.neo4j.kernel.haonefive.UrlUtil.toUrl;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Pair;

public class MockedDistributedElection
{
    private final Map<Integer, Member> members = new HashMap<Integer, Member>();
    private Member currentMaster;
    
    public Pair<URL, Integer> currentMaster()
    {
        return currentMaster == null ? null : Pair.of( currentMaster.masterUrl, currentMaster.id );
    }

    public void bluntlyForceMasterElection( int masterServerId )
    {
        Member master = members.get( masterServerId );
        for ( Member member : members.values() )
            member.client.distributeNewMasterElected( master.masterUrl, masterServerId );
        for ( Member member : members.values() )
            member.client.distributeNewMasterBecameAvailable( master.masterUrl );
        currentMaster = master;
    }

    public void addClient( MockedMasterElectionClient client, int serverId, int port )
    {
        members.put( serverId, new Member( serverId, client, port ) );
    }

    private static class Member
    {
        private final MockedMasterElectionClient client;
        private final URL masterUrl;
        private final int id;

        Member( int id, MockedMasterElectionClient client, int port )
        {
            this.id = id;
            this.client = client;
            this.masterUrl = toUrl( "localhost", port );
        }
    }
}
