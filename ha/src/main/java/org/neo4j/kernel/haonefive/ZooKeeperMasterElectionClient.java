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

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterEventReceiver;
import org.neo4j.kernel.ha.SlaveDatabaseOperations;
import org.neo4j.kernel.ha.shell.ZooClientFactory;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.impl.util.StringLogger;

public class ZooKeeperMasterElectionClient
        implements MasterElectionClient, ZooClientFactory, SlaveDatabaseOperations, ClusterEventReceiver
{
    private final Broker broker;
    private final Collection<MasterChangeListener> listeners = new CopyOnWriteArrayList<MasterChangeListener>();
    private final Config config;
    private final HaServiceSupplier stuff;
    private final StoreIdGetter storeIdGetter;
    private final String storeDir;
    
    public ZooKeeperMasterElectionClient( HaServiceSupplier stuff, Config config, StoreIdGetter storeIdGetter, String storeDir )
    {
        this.stuff = stuff;
        this.config = config;
        this.storeIdGetter = storeIdGetter;
        this.storeDir = storeDir;
        broker = new ZooKeeperBroker( config, this );
    }
    
    @Override
    public void initialJoin()
    {
    }
    
    @Override
    public void clearListeners()
    {
        listeners.clear();
    }
    
    @Override
    public void addListener( MasterChangeListener listener )
    {
        listeners.add( listener );
    }
    
    // For testing
    @Override
    public void bluntlyForceMasterElection()
    {
        /*
         * - Use ZooKeeper event to notify all instances that they need to update
         *   ZK with their master election data.
         * - Wait for all to update (what's the criteria for end wait?)
         * - Use the data in ZK to elect new master.
         * - Tell all listeners about the newly elected master.
         */
        
        try
        {
            broker.callForData();
            
            // TODO implement wait for real
            Thread.sleep( 2000 );
            
            Machine master = broker.getMasterReally( true ).other();
            System.out.println( "Elected " + master.getMachineId() );
            String masterId = "http://" + master.getServer().first() + ":" + master.getServer().other();
            int masterServerId = master.getMachineId();
            MyMasterBecameAvailableCallback callback = new MyMasterBecameAvailableCallback();
            for ( MasterChangeListener listener : listeners )
                listener.newMasterElected( masterId, masterServerId, callback );
            callback.waitFor();
            for ( MasterChangeListener listener : listeners )
                listener.newMasterBecameAvailable( masterId );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public ZooClient newZooClient()
    {
        return new ZooClient( storeDir, StringLogger.SYSTEM, storeIdGetter, config, this, this );
    }

    @Override
    public void handle( Exception e )
    {
    }

    @Override
    public SlaveContext getSlaveContext( int eventIdentifier )
    {
        return stuff.getSlaveContext( eventIdentifier );
    }

    @Override
    public <T> T receive( Response<T> response )
    {
        stuff.receive( response );
        return response.response();
    }

    @Override
    public void exceptionHappened( RuntimeException e )
    {
    }

    @Override
    public int getMasterForTx( long tx )
    {
        return stuff.getMasterIdForTx( tx );
    }

    @Override
    public void newMaster( Exception cause )
    {
    }

    @Override
    public void reconnect( Exception cause )
    {
        broker.restart();
    }
    
    private static class MyMasterBecameAvailableCallback implements MasterBecameAvailableCallback
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
}
