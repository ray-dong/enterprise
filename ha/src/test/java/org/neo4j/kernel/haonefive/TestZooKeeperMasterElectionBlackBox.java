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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.com.SlaveContext.lastAppliedTx;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Config.DEFAULT_DATA_SOURCE_NAME;

import java.net.URL;

import javax.transaction.Transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.SlaveContext.Tx;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestZooKeeperMasterElectionBlackBox
{
    private StoreIdGetter storeIdGetter;
    private LocalhostZooKeeperCluster zoo;
    private Instance[] instances;
    private Instance[] shutDownInstances;
    
    @Before
    public void before() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        storeIdGetter = new StoreIdGetter()
        {
            private StoreId storeId = new StoreId();
            
            @Override
            public StoreId get()
            {
                return storeId;
            }
        };
    }

    @After
    public void after() throws Exception
    {
        for ( Instance instance : instances )
            if ( instance != null )
                instance.shutdown();
    }
    
    @Test
    public void shouldElectSingleInstanceInClusterAsMaster() throws Exception
    {
        startCluster( 1 );
        assertEquals( 0, waitForMasterToBecomeAvailable( 0 ).lastMasterServerId );
    }
    
    @Test
    public void shouldElectMasterWithHighestTxId() throws Exception
    {
        startCluster( 3 );
        
        int currentMaster = getCurrentMaster();
        setLastTx( 0, 5, currentMaster );
        setLastTx( 1, 6, currentMaster );
        setLastTx( 2, 7, currentMaster );
        
        shutdownInstance( currentMaster );
        waitForMasterToBecomeAvailable( (currentMaster+1)%instances.length );
        assertEquals( rightfulMaster(), getCurrentMaster() );
    }
    
    @Test
    public void shouldReelectSameMasterIfEqualTxIds() throws Exception
    {
        startCluster( 3 );
        
        int currentMaster = getCurrentMaster();
        setLastTx( 0, 3, currentMaster );
        setLastTx( 1, 3, currentMaster );
        setLastTx( 2, 3, currentMaster );
        
        shutdownInstance( currentMaster );
        waitForMasterToBecomeAvailable( (currentMaster+1)%instances.length );
        int newMaster = getCurrentMaster();
        assertTrue( newMaster == (currentMaster+1)%instances.length || newMaster == (currentMaster+2)%instances.length );
        System.err.println( "start" );
        startInstance( currentMaster );
        Thread.sleep( 5000 );
        waitForMasterToBecomeAvailable( currentMaster );
        assertEquals( currentMaster, getCurrentMaster() );
    }
    
    private Instance newInstance( int id )
    {
        return new Instance( id, storeIdGetter, "target/db/" + id );
    }
    
    private void startInstance( int id )
    {
        Instance instance = newInstance( id );
        Instance previous = shutDownInstances[id];
        instance.setLastTx( previous.lastTx, previous.masterIdForLastTx );
        shutDownInstances[id] = null;
        instances[id] = instance;
        instance.client.requestMaster();
    }

    private int rightfulMaster()
    {
        Integer id = null;
        long tx = 0;
        for ( Instance instance : instances )
            if ( instance != null )
            {
                if ( id == null || instance.lastTx > tx )
                {
                    id = instance.id;
                    tx = instance.lastTx;
                }
            }
        return id;
    }
    
    private Instance waitForMasterToBecomeAvailable( int id )
    {
        Instance instance = instances[id];
        long end = currentTimeMillis() + SECONDS.toMillis( 10 );
        while ( currentTimeMillis() < end )
        {
            if ( instance.masterAvailable )
                return instance;
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
        throw new RuntimeException( "Timed out" );
    }
    
    private void shutdownInstance( int i )
    {
        for ( Instance instance : instances )
            if ( instance != null )
                instance.invalidateMaster();
        instances[i].shutdown();
        shutDownInstances[i] = instances[i];
        instances[i] = null;
    }

    private int getCurrentMaster()
    {
        Integer master = null;
        for ( Instance instance : instances )
        {
            if ( instance == null )
                continue;
            if ( master == null )
                master = instance.lastMasterServerId;
            else
                assertEquals( instance.lastMasterServerId, master.intValue() );
        }
        return master;
    }

    private void setLastTx( int serverId, long txId, int masterId )
    {
        Instance instance = instances[serverId];
        instance.setLastTx( txId, masterId );
    }
    
    private void startCluster( int size )
    {
        instances = new Instance[size];
        shutDownInstances = new Instance[size];
        for ( int i = 0; i < instances.length; i++ )
            instances[i] = newInstance( i );
        
        // This mimics what ha graphdb will do after its master election client has been created.
        for ( Instance instance : instances )
            instance.client.requestMaster();
    }

    private class Instance implements
            HaServiceSupplier, // Trim this down. ZooKeeperMasterElectionClient doesn't need it all
            MasterChangeListener
    {
        private final MasterElectionClient client;
        private long lastTx = 1;
        private int masterIdForLastTx = -1;
        private final int id;
        
        private volatile URL lastMasterUrl;
        private volatile int lastMasterServerId;
        private volatile boolean masterAvailable;
        
        Instance( int id, StoreIdGetter storeIdGetter, String storeDir )
        {
            this.id = id;
            this.client = new ZooKeeperMasterElectionClient( this, config( id ), storeIdGetter, storeDir );
            this.client.addMasterChangeListener( this );
        }

        public void invalidateMaster()
        {
            masterAvailable = false;
        }

        private Config config( int id )
        {
            Config config = new Config( new ConfigurationDefaults( HaSettings.class, GraphDatabaseSettings.class, OnlineBackupSettings.class ).apply( stringMap(
                    HaSettings.server_id.name(), "" + id,
                    HaSettings.coordinators.name(), zoo.getConnectionString()
                    ) ) );
            return config;
        }

        public void shutdown()
        {
            client.shutdown();
        }
        
        public void setLastTx( long lastTx, int masterId )
        {
            this.lastTx = lastTx;
            this.masterIdForLastTx = masterId;
        }

        @Override
        public Master getMaster()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SlaveContext getSlaveContext( int eventIdentifier )
        {
            return new SlaveContext( 0, id, eventIdentifier, new Tx[] { lastAppliedTx( DEFAULT_DATA_SOURCE_NAME, lastTx ) }, masterIdForLastTx, 0 );
        }

        @Override
        public SlaveContext getSlaveContext( XaDataSource dataSource )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SlaveContext getSlaveContext()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SlaveContext getEmptySlaveContext()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void receive( Response<?> response )
        {
        }

        @Override
        public boolean hasAnyLocks( Transaction tx )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMasterServerId()
        {
            return id;
        }

        @Override
        public void makeSureTxHasBeenInitialized()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMasterIdForTx( long tx )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Triplet<Long, Integer, Long> getLastTx()
        {
            return Triplet.of( lastTx, masterIdForLastTx, 0L );
        }

        @Override
        public void newMasterElected( URL masterUrl, int masterServerId )
        {
            this.masterAvailable = false;
            this.lastMasterUrl = masterUrl;
            this.lastMasterServerId = masterServerId;
        }

        @Override
        public void newMasterBecameAvailable( URL masterUrl )
        {
            assertEquals( this.lastMasterUrl, masterUrl );
            this.masterAvailable = true;
        }
    }
}
