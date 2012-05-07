package org.neo4j.kernel.haonefive;

import java.net.URL;

import org.neo4j.helpers.Pair;

public class MockedMasterElectionClient extends AbstractMasterElectionClient
{
    final MockedDistributedElection distributed;

    public MockedMasterElectionClient( MockedDistributedElection distributed )
    {
        this.distributed = distributed;
    }
    
    @Override
    public void requestMaster()
    {
        Pair<URL, Integer> currentMaster = distributed.currentMaster();
        if ( currentMaster != null )
        {
            distributeNewMasterElected( currentMaster.first(), currentMaster.other() );
            distributeNewMasterBecameAvailable( currentMaster.first() );
        }
    }

    @Override
    public MasterElectionInput askForMasterElectionInput()
    {
        throw new UnsupportedOperationException( "Not needed when mocked" );
    }

    @Override
    public void shutdown()
    {
    }

    public void distributeNewMasterElected( URL masterUrl, int masterServerId )
    {
        for ( MasterChangeListener listener : listeners )
            listener.newMasterElected( masterUrl, masterServerId );
    }

    public void distributeNewMasterBecameAvailable( URL masterUrl )
    {
        for ( MasterChangeListener listener : listeners )
            listener.newMasterBecameAvailable( masterUrl );
    }
}
