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

import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.LockStatus;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;

/**
 * Acts as a {@link Master} for when the master is myself, i.e. it delegates back
 * to default behavior of services calling these methods.
 */
public class LoopbackMaster implements Master
{
    private final StoreId storeId;
    private final XaDataSourceManager dsManager;
    private final RelationshipTypeCreator relationshipTypeCreator;
    private final TxManager txManager;
    private final EntityIdGenerator entityIdGenerator;
    private final PersistenceManager persistenceManager;
    private final RelationshipTypeHolder relationshipTypeHolder;

    public LoopbackMaster( StoreId storeId, XaDataSourceManager dsManager, RelationshipTypeCreator relationshipTypeCreator,
            TxManager txManager, EntityIdGenerator entityIdGenerator, PersistenceManager persistenceManager,
            RelationshipTypeHolder relationshipTypeHolder )
    {
        this.storeId = storeId;
        this.dsManager = dsManager;
        this.relationshipTypeCreator = relationshipTypeCreator;
        this.txManager = txManager;
        this.entityIdGenerator = entityIdGenerator;
        this.persistenceManager = persistenceManager;
        this.relationshipTypeHolder = relationshipTypeHolder;
    }
    
    @Override
    public Response<IdAllocation> allocateIds( IdType idType )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Integer> createRelationshipType( SlaveContext context, String name )
    {
        int id = relationshipTypeCreator.getOrCreate( txManager, entityIdGenerator, persistenceManager, relationshipTypeHolder, name );
        return emptyResponse( id );
    }

    @Override
    public Response<Void> initializeTx( SlaveContext context )
    {
        return emptyResponse( null );
    }

    @Override
    public Response<LockResult> acquireNodeWriteLock( SlaveContext context, long... nodes )
    {
        return emptyLockResponse();
    }

    @Override
    public Response<LockResult> acquireNodeReadLock( SlaveContext context, long... nodes )
    {
        return emptyLockResponse();
    }

    @Override
    public Response<LockResult> acquireGraphWriteLock( SlaveContext context )
    {
        return emptyLockResponse();
    }

    @Override
    public Response<LockResult> acquireGraphReadLock( SlaveContext context )
    {
        return emptyLockResponse();
    }

    @Override
    public Response<LockResult> acquireRelationshipWriteLock( SlaveContext context, long... relationships )
    {
        return emptyLockResponse();
    }

    @Override
    public Response<LockResult> acquireRelationshipReadLock( SlaveContext context, long... relationships )
    {
        return emptyLockResponse();
    }

    @Override
    public Response<Long> commitSingleResourceTransaction( SlaveContext context, String resource, TxExtractor txGetter )
    {
        long txId = TxIdGenerator.DEFAULT.generate( dsManager.getXaDataSource( resource ), context.getEventIdentifier() );
        return emptyResponse( txId );
    }

    @Override
    public Response<Void> finishTransaction( SlaveContext context, boolean success )
    {
        return emptyResponse( null );
    }

    @Override
    public Response<Void> pullUpdates( SlaveContext context )
    {
        return emptyResponse( null );
    }

    @Override
    public Response<Pair<Integer, Long>> getMasterIdForCommittedTx( long txId, StoreId myStoreId )
    {
        try
        {
            Pair<Integer, Long> result = dsManager.getNeoStoreDataSource().getMasterForCommittedTx( txId );
            return emptyResponse( result );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Response<Void> copyStore( SlaveContext context, StoreWriter writer )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Response<Void> copyTransactions( SlaveContext context, String dsName, long startTxId, long endTxId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( SlaveContext context, String index, String key )
    {
        return emptyLockResponse();
    }

    @Override
    public Response<LockResult> acquireIndexReadLock( SlaveContext context, String index, String key )
    {
        return emptyLockResponse();
    }

    private <T> Response<T> emptyResponse( T responseValue )
    {
        return new Response<T>( responseValue , storeId, TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    private Response<LockResult> emptyLockResponse()
    {
        return emptyResponse( new LockResult( LockStatus.OK_LOCKED ) );
    }
}
