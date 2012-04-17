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
package org.neo4j.kernel.ha;

import java.util.List;

import javax.transaction.Transaction;

import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.NodeManager.IndexLock;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.LockNotFoundException;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.info.LockInfo;

public class SlaveLockManager implements LockManager
{
    private final Broker broker;
    private final TxManager tm;
    private final SlaveDatabaseOperations databaseOperations;
    private final TxHook txHook;
    private final LockManager local;

    public SlaveLockManager( RagManager ragManager, TxManager tm, TxHook txHook, Broker broker,
            SlaveDatabaseOperations databaseOperations )
    {
        this.local = new LockManagerImpl( ragManager );
        this.tm = tm;
        this.txHook = txHook;
        this.broker = broker;
        this.databaseOperations = databaseOperations;
    }

    private int getLocalTxId()
    {
        return tm.getEventIdentifier();
    }

    public long getDetectedDeadlockCount()
    {
        return local.getDetectedDeadlockCount();
    }

    public void releaseReadLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException
    {
        local.releaseReadLock( resource, tx );
    }

    public void releaseWriteLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException
    {
        local.releaseWriteLock( resource, tx );
    }

    public void dumpLocksOnResource( Object resource )
    {
        local.dumpLocksOnResource( resource );
    }

    public List<LockInfo> getAllLocks()
    {
        return local.getAllLocks();
    }

    public List<LockInfo> getAwaitedLocks( long minWaitTime )
    {
        return local.getAwaitedLocks( minWaitTime );
    }

    public void dumpRagStack()
    {
        local.dumpRagStack();
    }

    public void dumpAllLocks()
    {
        local.dumpAllLocks();
    }

    @Override
    public void getReadLock( Object resource ) throws DeadlockDetectedException,
            IllegalResourceException
    {
        LockGrabber grabber = null;
        if ( resource instanceof Node ) grabber = LockGrabber.NODE_READ;
        else if ( resource instanceof Relationship ) grabber = LockGrabber.RELATIONSHIP_READ;
        else if ( resource instanceof GraphProperties ) grabber = LockGrabber.GRAPH_READ;
        else if ( resource instanceof IndexLock ) grabber = LockGrabber.INDEX_READ;

        try
        {
            if ( grabber == null )
            {
                local.getReadLock( resource );
                return;
            }

            initializeTxIfFirst();
            LockResult result = null;
            do
            {
                int eventIdentifier = getLocalTxId();
                result = databaseOperations.receive( grabber.acquireLock( broker.getMaster().first(),
                        databaseOperations.getSlaveContext( eventIdentifier ), resource ) );
                switch ( result.getStatus() )
                {
                case OK_LOCKED:
                    local.getReadLock( resource );
                    return;
                case DEAD_LOCKED:
                    throw new DeadlockDetectedException( result.getDeadlockMessage() );
                }
            }
            while ( result.getStatus() == LockStatus.NOT_LOCKED );
        }
        catch ( RuntimeException e )
        {
            databaseOperations.exceptionHappened( e );
            throw e;
        }
    }

    private void initializeTxIfFirst()
    {
        // The main point of initializing transaction (for HA) is in TransactionImpl, so this is
        // for that extra point where grabbing a lock
        Transaction tx = tm.getTransaction();
        if ( !txHook.hasAnyLocks( tx ) ) txHook.initializeTransaction( tm.getEventIdentifier() );
    }

    @Override
    public void getWriteLock( Object resource ) throws DeadlockDetectedException,
            IllegalResourceException
    {
        // Code copied from getReadLock. Fix!
        LockGrabber grabber = null;
        if ( resource instanceof Node ) grabber = LockGrabber.NODE_WRITE;
        else if ( resource instanceof Relationship ) grabber = LockGrabber.RELATIONSHIP_WRITE;
        else if ( resource instanceof GraphProperties ) grabber = LockGrabber.GRAPH_WRITE;
        else if ( resource instanceof IndexLock ) grabber = LockGrabber.INDEX_WRITE;

        try
        {
            if ( grabber == null )
            {
                local.getWriteLock( resource );
                return;
            }

            initializeTxIfFirst();
            LockResult result = null;
            do
            {
                int eventIdentifier = getLocalTxId();
                result = databaseOperations.receive( grabber.acquireLock( broker.getMaster().first(),
                        databaseOperations.getSlaveContext( eventIdentifier ), resource ) );
                switch ( result.getStatus() )
                {
                case OK_LOCKED:
                    local.getWriteLock( resource );
                    return;
                case DEAD_LOCKED:
                    throw new DeadlockDetectedException( result.getDeadlockMessage() );
                }
            }
            while ( result.getStatus() == LockStatus.NOT_LOCKED );
        }
        catch ( RuntimeException e )
        {
            databaseOperations.exceptionHappened( e );
            throw e;
        }
    }

    // Release lock is as usual, since when the master committs it will release
    // the locks there and then when this slave committs it will release its
    // locks as usual here.

    private static enum LockGrabber
    {
        NODE_READ
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                return master.acquireNodeReadLock( context, ((Node)resource).getId() );
            }
        },
        NODE_WRITE
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                return master.acquireNodeWriteLock( context, ((Node)resource).getId() );
            }
        },
        RELATIONSHIP_READ
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                return master.acquireRelationshipReadLock( context, ((Relationship)resource).getId() );
            }
        },
        RELATIONSHIP_WRITE
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                return master.acquireRelationshipWriteLock( context, ((Relationship)resource).getId() );
            }
        },
        GRAPH_READ
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                return master.acquireGraphReadLock( context );
            }
        },
        GRAPH_WRITE
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                return master.acquireGraphWriteLock( context );
            }
        },
        INDEX_WRITE
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                IndexLock lock = (IndexLock) resource;
                return master.acquireIndexWriteLock( context, lock.getIndex(), lock.getKey() );
            }
        },
        INDEX_READ
        {
            @Override
            Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource )
            {
                IndexLock lock = (IndexLock) resource;
                return master.acquireIndexReadLock( context, lock.getIndex(), lock.getKey() );
            }
        };

        abstract Response<LockResult> acquireLock( Master master, SlaveContext context, Object resource );
    }
}
