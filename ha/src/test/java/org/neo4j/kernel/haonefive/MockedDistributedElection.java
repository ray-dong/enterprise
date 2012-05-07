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
