package org.neo4j.kernel.haonefive;

/**
 * Sits closest to the distributed master election algorithm and can provide input to
 * it and also delegate events to listeners.
 * 
 * (DISTRIBUTED MASTER ELECTION THINGIE) ----- (MasterElectionClient for db 1) ----- (listener 1)
 *                 |                                                    |----- (listener 2)
 *                 |
 *     (MasterElectionClient for db 2)
 *                 |
 *                 |
 *           (listener 3)
 */
public interface MasterElectionClient
{
    /**
     * Plead to get pinged about a current master, if any. Happens at startup of the database.
     */
    void requestMaster();
    
    /**
     * Ask this instance for input needed for doing a correct master election. Called when
     * a new master needs to be elected.
     */
    MasterElectionInput askForMasterElectionInput();
    
    void addMasterChangeListener( MasterChangeListener listener );
    
    void shutdown();
}
