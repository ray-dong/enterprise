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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.com.Client.ConnectionLostHandler;
import org.neo4j.com.MasterUtil;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.SlaveContext.Tx;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.BranchedDataPolicy;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class HaOneFiveGraphDb extends AbstractGraphDatabase implements MasterChangeListener
{
    final HaServiceSupplier stuff;
    private final int serverId;
    private volatile long sessionTimestamp;
    private volatile long lastUpdated;
    
    private volatile Master master;
    private volatile int masterServerId;
    private volatile DatabaseState databaseState = DatabaseState.TBD;
    private volatile MasterServer server;
    final MasterElectionClient masterElectionClient;
    
    // TODO This is an artifact of integrating with legacy code (MasterClient should change to not use this).
    private final StoreIdGetter storeIdGetter = new StoreIdGetter()
    {
        @Override
        public StoreId get()
        {
            return storeId;
        }
    };
    
    public HaOneFiveGraphDb( String storeDir, Map<String, String> params )
    {
        super( storeDir, (params = withDefaults( params )), Service.load( IndexProvider.class ),
                Service.load( KernelExtension.class ), Service.load( CacheProvider.class ) );
        
        serverId = new Config( params ).getInteger( HaSettings.server_id );
        sessionTimestamp = System.currentTimeMillis();
        stuff = new HaServiceSupplier()
        {
            @Override
            public void receive( Response<?> response )
            {
                try
                {
                    MasterUtil.applyReceivedTransactions( response, HaOneFiveGraphDb.this, MasterUtil.NO_ACTION );
                    lastUpdated = System.currentTimeMillis();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
            
            @Override
            public boolean hasAnyLocks( Transaction tx )
            {
                return lockReleaser.hasLocks( tx );
            }
            
            @Override
            public SlaveContext getSlaveContext()
            {
                return getSlaveContext( txManager.getEventIdentifier() );
            }
            
            @Override
            public SlaveContext getSlaveContext( XaDataSource dataSource )
            {
                return MasterUtil.getSlaveContext( dataSource, sessionTimestamp, serverId, txManager.getEventIdentifier() );
            }
            
            @Override
            public SlaveContext getSlaveContext( int eventIdentifier )
            {
                return MasterUtil.getSlaveContext( xaDataSourceManager, sessionTimestamp, serverId, eventIdentifier );
            }
            
            @Override
            public SlaveContext getEmptySlaveContext()
            {
                return new SlaveContext( 0, serverId, 0, new Tx[0], 0, 0 );
            }
            
            @Override
            public int getMasterServerId()
            {
                return masterServerId;
            }
            
            @Override
            public Master getMaster()
            {
                databaseState.beforeGetMaster( HaOneFiveGraphDb.this );
                return master;
            }

            @Override
            public void makeSureTxHasBeenInitialized()
            {
                try
                {
                    Transaction tx = txManager.getTransaction();
                    int eventIdentifier = txManager.getEventIdentifier();
                    if ( !hasAnyLocks( tx ) ) txHook.initializeTransaction( eventIdentifier );
                }
                catch ( SystemException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public int getMasterIdForTx( long tx )
            {
                try
                {
                    return getXaDataSourceManager().getNeoStoreDataSource().getMasterForCommittedTx( tx ).first().intValue();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
        
        run();
        
        masterElectionClient = createMasterElectionClient();
        masterElectionClient.addListener( this );
        masterElectionClient.initialJoin();
    }
    
    private static Map<String, String> withDefaults( Map<String, String> params )
    {
        params = new ConfigurationDefaults( HaSettings.class ).apply( params );
        params.put( Config.KEEP_LOGICAL_LOGS, "true" );
        return params;
    }

    protected MasterElectionClient createMasterElectionClient()
    {
        return new ZooKeeperMasterElectionClient( stuff, config, storeIdGetter, storeDir );
    }
    
    @Override
    protected TxHook createTxHook()
    {
        return new HaTxHook( stuff );
    }
    
    @Override
    protected TxIdGenerator createTxIdGenerator()
    {
        return new HaTxIdGenerator( stuff, serverId );
    }
    
    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new HaIdGeneratorFactory( stuff, serverId );
    }
    
    @Override
    protected LockManager createLockManager()
    {
        return new HaLockManager( stuff, ragManager );
    }
    
    @Override
    protected RelationshipTypeCreator createRelationshipTypeCreator()
    {
        return new HaRelationshipTypeCreator( stuff );
    }

    @Override
    public void newMasterElected( String masterUrl, int masterServerId, MasterBecameAvailableCallback callback )
    {
        System.out.println( this + " newMasterElected " + masterServerId );
        
        // TODO Block incoming transactions and rollback active ones or something.
        
        URL url;
        try
        {
            url = new URL( masterUrl );
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException( e );
        }
        
        boolean iAmToBecomeMaster = masterServerId == this.serverId;
        if ( iAmToBecomeMaster )
        {
            databaseState.becomeMaster( this, url.getPort() );
            databaseState = DatabaseState.MASTER;
        }
        else
        {
            databaseState.becomeSlave( this, url.getHost(), url.getPort() );
            databaseState = DatabaseState.SLAVE;
        }
        System.out.println( this + " databaseState set to " + databaseState );
        this.masterServerId = masterServerId;
        ((HaIdGeneratorFactory) idGeneratorFactory).masterChanged( master, masterServerId );
        
        if ( iAmToBecomeMaster )
            callback.iAmMasterNowAndReady();
    }

    @Override
    public void newMasterBecameAvailable( String masterUrl )
    {
        // TODO Remove blockade
    }
    
    @Override
    public MasterElectionInput askForMasterElectionInput()
    {
        try
        {
            NeoStoreXaDataSource neoStoreDataSource = getXaDataSourceManager().getNeoStoreDataSource();
            long tx = neoStoreDataSource.getLastCommittedTxId();
            Pair<Integer, Long> masterInfo = neoStoreDataSource.getMasterForCommittedTx( tx );
            return new MasterElectionInput( tx, masterInfo.first() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void pullUpdates()
    {
        Response<Void> response = master.pullUpdates( stuff.getSlaveContext() );
        stuff.receive( response );
    }

    enum DatabaseState
    {
        TBD
        {
            @Override
            void handleWriteOperation( HaOneFiveGraphDb db )
            {
                // TODO Block and wait for db to decide role a while and return
                // If it couldn't be decided throw exception
            }

            @Override
            void becomeMaster( HaOneFiveGraphDb db, int port )
            {
                db.server = newServer( db, port );
                db.master = newLoopbackMaster( db );
            }

            @Override
            void becomeSlave( final HaOneFiveGraphDb db, String masterIp, int masterPort )
            {
                db.master = newClient( db, masterIp, masterPort );
                
                // TODO If my db has a different store id than the master copy it. This happens
                // when we start up for the first time and the AbstractGraphDatabase constructor
                // creates an empty db.
                if ( true /* store id differs from master*/ )
                {
                    if ( true /*my db is empty*/ )
                    {
                        System.out.println( "Copying store" );
                        db.life.stop();
                        try
                        {
                            BranchedDataPolicy.keep_none.handle( db ); // Will delete the relevant store files
                            Response<Void> response = db.master.copyStore( db.stuff.getEmptySlaveContext(), new ToFileStoreWriter( db.storeDir ) );
                            db.stuff.receive( response );
                        }
                        finally
                        {
                            db.life.start();
                        }
                        System.out.println( "Copied store" );
                    }
                    else
                    {
                        // TODO You're stupid
                    }
                }
                else
                {
                    // TODO Verify data consistency with master
                }
            }

            @Override
            void beforeGetMaster( HaOneFiveGraphDb db )
            {
                new Exception().printStackTrace();
                
                // Wait for a master/slave decision
                long endTime = currentTimeMillis() + SECONDS.toMillis( 20 );
                try
                {
                    while ( currentTimeMillis() < endTime )
                    {
                        if ( db.databaseState != this )
                            return;
                        Thread.sleep( 100 );
                        System.out.println( db + " waiting for role decision " + db.databaseState );
                    }
                    throw new RuntimeException( "No role decision was made" );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    throw new RuntimeException( e );
                }
            }
        },
        MASTER
        {
            @Override
            void handleWriteOperation( HaOneFiveGraphDb db )
            {
            }

            @Override
            void becomeMaster( HaOneFiveGraphDb db, int port )
            {
                // Do nothing, I'm already master
            }

            @Override
            void becomeSlave( HaOneFiveGraphDb db, String masterIp, int masterPort )
            {
                // TODO Switch to slave
                db.server.shutdown();
                db.server = null;
                db.master.shutdown();
                db.master = newClient( db, masterIp, masterPort );
                // TODO Verify data consistency with master
            }

            @Override
            void beforeGetMaster( HaOneFiveGraphDb db )
            {
            }
        },
        SLAVE
        {
            @Override
            void handleWriteOperation( HaOneFiveGraphDb db )
            {
            }

            @Override
            void becomeMaster( HaOneFiveGraphDb db, int port )
            {
                db.server = newServer( db, port );
                db.master.shutdown();
                db.master = newLoopbackMaster( db );
            }

            @Override
            void becomeSlave( HaOneFiveGraphDb db, String masterIp, int masterPort )
            {
                db.master.shutdown();
                db.master = newClient( db, masterIp, masterPort );
                // TODO Verify data consistency with master
            }

            @Override
            void beforeGetMaster( HaOneFiveGraphDb db )
            {
            }
        };
        
        abstract void becomeMaster( HaOneFiveGraphDb db, int port );
        
        abstract void beforeGetMaster( HaOneFiveGraphDb db );

        protected Master newLoopbackMaster( HaOneFiveGraphDb db )
        {
            return new LoopbackMaster( db.storeId, db.xaDataSourceManager,
                    db.txManager, db.persistenceSource, db.persistenceManager, db.relationshipTypeHolder );
        }

        protected MasterServer newServer( HaOneFiveGraphDb db, int port )
        {
            return new MasterServer( new MasterImpl( db, 20 ), port, db.getMessageLog(),
                    20, 20, TxChecksumVerifier.ALWAYS_MATCH );
        }

        protected Master newClient( HaOneFiveGraphDb db, String masterIp, int masterPort )
        {
            // TODO Wrap returned Master in something that handles exceptions (network a.s.o.)
            // and feeds back to master election black box if we decide to have input channels to it.
            return new MasterClient( masterIp, masterPort, db.getMessageLog(), db.storeIdGetter,
                    ConnectionLostHandler.NO_ACTION, 20, 20, 20 );
        }

        abstract void becomeSlave( HaOneFiveGraphDb db, String masterIp, int masterPort );
        
        abstract void handleWriteOperation( HaOneFiveGraphDb db );
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + serverId + ", " + storeDir + "]";
    }
}
