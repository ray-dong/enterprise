package org.neo4j.kernel.haonefive;

import java.util.HashMap;
import java.util.Map;

public class MockedMasterElectionClient extends AbstractMasterElectionClient
{
    @Override
    public void requestMaster()
    {
        if ( currentMaster != null )
        {
            for ( Member member : listeners.values() )
                member.listener.newMasterElected( currentMaster.masterUrl, currentMaster.id );
            for ( Member member : listeners.values() )
                member.listener.newMasterBecameAvailable( currentMaster.masterUrl );
        }
    }

    public void bluntlyForceMasterElection( int masterServerId )
    {
        Member master = listeners.get( masterServerId );
        for ( Member member : listeners.values() )
            member.listener.newMasterElected( master.masterUrl, masterServerId );
        for ( Member member : listeners.values() )
            member.listener.newMasterBecameAvailable( master.masterUrl );
        currentMaster = master;
    }

    @Override
    public MasterElectionInput askForMasterElectionInput()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void addListener( MasterChangeListener listener, int id, int port )
    {
        listeners.put( id, new Member( id, listener, port ) );
    }
    
    public void removeListener( int id )
    {
        listeners.remove( id );
    }
    
    private static class Member
    {
        private final MasterChangeListener listener;
        private final String masterUrl;
        private final int id;

        Member( int id, MasterChangeListener listener, int port )
        {
            this.id = id;
            this.listener = listener;
            this.masterUrl = "http://localhost:" + port;
        }
    }
}
