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
import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.TransactionBuilder;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;

public class HaOneFiveGraphDb implements GraphDatabaseAPI, MasterChangeListener
{
    private final Config config;
    private volatile InternalHaOneFiveGraphDb delegate;
    private HashMap<String, String> params;
    
    public HaOneFiveGraphDb( String storeDir, Map<String, String> inputParams )
    {
        config = createConfig( inputParams );
        
        // Here we're in a state awaiting a call from master change listener...
    }

    private Config createConfig( Map<String, String> inputParams )
    {
        HashMap<String, String> params = new HashMap<String, String>( inputParams );
        new ConfigurationDefaults( GraphDatabaseSettings.class, HaSettings.class, OnlineBackupSettings.class ).apply( params );
        this.params = params;
        return new Config( params );
    }

    private InternalHaOneFiveGraphDb delegate()
    {
        // TODO if delegate not available wait until it's available
        return delegate;
    }

    public void masterChanged( Master master, int masterServerId )
    {
        if ( delegate == null )
        {
            // TODO Copy store if needed
            // TODO Check consistency with master
            delegate = new InternalHaOneFiveGraphDb( getStoreDir(), params, master, masterServerId );
        }
        else
        {
            // TODO Check consistency with master
            delegate.masterChanged( master, masterServerId );
        }
    }

    public void shutdown()
    {
        delegate().shutdown();
    }

    public final String getStoreDir()
    {
        return delegate().getStoreDir();
    }

    public StoreId getStoreId()
    {
        return delegate().getStoreId();
    }

    public Transaction beginTx()
    {
        return delegate().beginTx();
    }

    public boolean transactionRunning()
    {
        return delegate().transactionRunning();
    }

    public final <T> T getManagementBean( Class<T> type )
    {
        return delegate().getManagementBean( type );
    }

    public final <T> T getSingleManagementBean( Class<T> type )
    {
        return delegate().getSingleManagementBean( type );
    }

    public String toString()
    {
        return delegate().toString();
    }

    public Iterable<Node> getAllNodes()
    {
        return delegate().getAllNodes();
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return delegate().getRelationshipTypes();
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return delegate().registerKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        return delegate().registerTransactionEventHandler( handler );
    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return delegate().unregisterKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        return delegate().unregisterTransactionEventHandler( handler );
    }

    public Node createNode()
    {
        return delegate().createNode();
    }

    public Node getNodeById( long id )
    {
        return delegate().getNodeById( id );
    }

    public Relationship getRelationshipById( long id )
    {
        return delegate().getRelationshipById( id );
    }

    public Node getReferenceNode()
    {
        return delegate().getReferenceNode();
    }

    public TransactionBuilder tx()
    {
        return delegate().tx();
    }

    public Guard getGuard()
    {
        return delegate().getGuard();
    }

    public <T> Collection<T> getManagementBeans( Class<T> beanClass )
    {
        return delegate().getManagementBeans( beanClass );
    }

    public KernelData getKernelData()
    {
        return delegate().getKernelData();
    }

    public IndexManager index()
    {
        return delegate().index();
    }

    public Config getConfig()
    {
        return delegate().getConfig();
    }

    public NodeManager getNodeManager()
    {
        return delegate().getNodeManager();
    }

    public LockReleaser getLockReleaser()
    {
        return delegate().getLockReleaser();
    }

    public LockManager getLockManager()
    {
        return delegate().getLockManager();
    }

    public XaDataSourceManager getXaDataSourceManager()
    {
        return delegate().getXaDataSourceManager();
    }

    public TransactionManager getTxManager()
    {
        return delegate().getTxManager();
    }

    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return delegate().getRelationshipTypeHolder();
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return delegate().getIdGeneratorFactory();
    }

    public DiagnosticsManager getDiagnosticsManager()
    {
        return delegate().getDiagnosticsManager();
    }

    public PersistenceSource getPersistenceSource()
    {
        return delegate().getPersistenceSource();
    }

    public final StringLogger getMessageLog()
    {
        return delegate().getMessageLog();
    }

    public KernelPanicEventGenerator getKernelPanicGenerator()
    {
        return delegate().getKernelPanicGenerator();
    }

    public boolean equals( Object o )
    {
        return delegate().equals( o );
    }

    public int hashCode()
    {
        return delegate().hashCode();
    }
}
