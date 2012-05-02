package org.neo4j.kernel.haonefive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZooKeeperMasterElection
{
    private Instance[] instances;
    
    @Before
    public void before() throws Exception
    {
        instances = new Instance[3];
        for ( int i = 0; i < instances.length; i++ )
            instances[i] = new Instance( new ZooKeeperMasterElectionClient( null, null, null, null, null ), null );
    }

    @After
    public void after() throws Exception
    {
        for ( Instance instance : instances )
            instance.shutdown();
    }
    
    @Test
    public void shouldElectRightMaster() throws Exception
    {
        
    }
    
    @Test
    public void shouldReelectMasterWhenCurrentMasterIsRemoved() throws Exception
    {
        
    }
    
    private static class Instance
    {
        private final MasterElectionClient client;
        private final MasterChangeListener listener;
        
        Instance( MasterElectionClient client, MasterChangeListener listener )
        {
            this.client = client;
            this.listener = listener;
        }

        public void shutdown()
        {
            client.shutdown();
        }
    }
}
