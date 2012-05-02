package org.neo4j.kernel.haonefive;

public interface MasterElectionClient
{
    /**
     * Plead to get pinged about a current master, if any. Happens at startup of the database.
     */
    void requestMaster();
    void shutdown();
}
