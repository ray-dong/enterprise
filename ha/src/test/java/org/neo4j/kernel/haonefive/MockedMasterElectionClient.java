package org.neo4j.kernel.haonefive;

import java.util.HashMap;
import java.util.Map;

public class MockedMasterElectionClient implements MasterElectionClient
{
    private final Map<Integer,Member> listeners = new HashMap<Integer, Member>();
    private Member currentMaster;
    
    @Override
    public void requestMaster()
    {
    }

    @Override
    public void shutdown()
    {
    }

    public void bluntlyForceMasterElection( int masterServerId )
    {
        try
        {
            Member master = listeners.get( masterServerId );
            String masterId = "http://" + "localhost" + ":" + master.port;
            MyMasterBecameAvailableCallback callback = new MyMasterBecameAvailableCallback();
            for ( Member member : listeners.values() )
                member.listener.newMasterElected( masterId, masterServerId, callback );
            callback.waitFor();
            for ( Member member : listeners.values() )
                member.listener.newMasterBecameAvailable( masterId );
            currentMaster = master;
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( e );
        }
    }

    public void addListener( MasterChangeListener listener, int id, int port )
    {
        listeners.put( id, new Member( listener, port ) );
    }
    
    public void removeListener( int id )
    {
        listeners.remove( id );
    }
    
    private static class Member
    {
        private final MasterChangeListener listener;
        private final int port;

        Member( MasterChangeListener listener, int port )
        {
            this.listener = listener;
            this.port = port;
        }
    }
}
