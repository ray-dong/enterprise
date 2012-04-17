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

import java.io.IOException;
import java.util.Map;

import javax.transaction.Transaction;

import org.neo4j.com.MasterUtil;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class HaOneFiveGraphDb extends AbstractGraphDatabase implements MasterChangeListener
{
    private final HaServiceSupplier stuff;
    private final int serverId;
    private volatile long sessionTimestamp;
    
    private volatile Master master;
    private volatile int masterServerId;
    
    public HaOneFiveGraphDb( String storeDir, Map<String, String> params )
    {
        super( storeDir, params, Service.load( IndexProvider.class ),
                Service.load( KernelExtension.class ), Service.load( CacheProvider.class ) );
        
        serverId = new Config( params ).getInteger( HaSettings.server_id );
        
        stuff = new HaServiceSupplier()
        {
            @Override
            public void receive( Response<?> response )
            {
                try
                {
                    MasterUtil.applyReceivedTransactions( response, HaOneFiveGraphDb.this, MasterUtil.NO_ACTION );
                    sessionTimestamp = System.currentTimeMillis();
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
            public SlaveContext getSlaveContext( int identifier, XaDataSource dataSource )
            {
                return MasterUtil.getSlaveContext( dataSource, sessionTimestamp, serverId, identifier );
            }
            
            @Override
            public SlaveContext getSlaveContext( int eventIdentifier )
            {
                return MasterUtil.getSlaveContext( xaDataSourceManager, sessionTimestamp, serverId, eventIdentifier );
            }
            
            @Override
            public int getServerId()
            {
                return serverId;
            }
            
            @Override
            public int getMasterServerId()
            {
                return masterServerId;
            }
            
            @Override
            public Master getMaster()
            {
                return master;
            }
        };
        
        run();
    }
    
    /* Services:
     * - tx hook
     * - tx id generator
     * - id generator factory
     * - lock manager
     */
    
    @Override
    protected TxHook createTxHook()
    {
        return new HaTxHook( stuff );
    }
    
    @Override
    protected TxIdGenerator createTxIdGenerator()
    {
        return new HaTxIdGenerator( stuff );
    }
    
    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new HaIdGeneratorFactory( stuff );
    }
    
    @Override
    protected LockManager createLockManager()
    {
        return new HaLockManager( stuff, ragManager );
    }

    @Override
    public void masterChanged( Master master, int masterServerId )
    {
        // Block transactions
        ((HaIdGeneratorFactory) idGeneratorFactory).masterChanged( master, masterServerId );
        this.masterServerId = masterServerId;
        this.master = master;
        // Remove block
    }
}
