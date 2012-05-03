package org.neo4j.kernel.haonefive;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

abstract class AbstractMasterElectionClient implements MasterElectionClient
{
    protected final List<MasterChangeListener> listeners = new CopyOnWriteArrayList<MasterChangeListener>();

    @Override
    public void addMasterChangeListener( MasterChangeListener listener )
    {
        listeners.add( listener );
    }
}
