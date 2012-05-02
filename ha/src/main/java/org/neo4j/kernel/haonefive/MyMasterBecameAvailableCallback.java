package org.neo4j.kernel.haonefive;

class MyMasterBecameAvailableCallback implements MasterBecameAvailableCallback
{
    private volatile boolean called;
    
    @Override
    public synchronized void iAmMasterNowAndReady()
    {
        notify();
        called = true;
    }
    
    public synchronized void waitFor() throws InterruptedException
    {
        if ( !called )
            wait( 10000 );
    }
}