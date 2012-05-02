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

import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterEventReceiver;
import org.neo4j.kernel.ha.SlaveDatabaseOperations;
import org.neo4j.kernel.ha.shell.ZooClientFactory;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.impl.util.StringLogger;

public class ZooKeeperMasterElectionClient
        implements MasterElectionClient, ZooClientFactory, SlaveDatabaseOperations, ClusterEventReceiver
{
    private final Broker broker;
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
    public void requestMaster()
    {
//        /* TODO remember that this is a spike.
//         * Spawn off a thread which waits a while (for the case where all dbs start at the same time)
//         * then asks ZK who is the master and sends out a "new master" event */
//        new Thread()
//        {
//            @Override
//            public void run()
//            {
//                try
//                {
//                    Thread.sleep( 2000 );
//                    Machine master = broker.getMasterReally( true ).other();
//                    String masterUrl = "http://" + master.getServer().first() + ":" + master.getServer().other();
//                    for ( MasterChangeListener listener : listeners )
//                        listener.newMasterElected( masterUrl, master.getMachineId(), new MyMasterBecameAvailableCallback() );
//                    for ( MasterChangeListener listener : listeners )
//                        listener.newMasterBecameAvailable( masterUrl );
//                }
//                catch ( InterruptedException e )
//                {
//                    System.out.println( e );
//                    Thread.interrupted();
//                }
//            }
//        }.start();
    }
    
//    private void electMasterAndPingListeners()
//    {
//        try
//        {
//            Machine master = broker.getMasterReally( true ).other();
//            String masterId = "http://" + master.getServer().first() + ":" + master.getServer().other();
//            int masterServerId = master.getMachineId();
//            MyMasterBecameAvailableCallback callback = new MyMasterBecameAvailableCallback();
//            for ( MasterChangeListener listener : listeners )
//                listener.newMasterElected( masterId, masterServerId, callback );
//            callback.waitFor();
//            for ( MasterChangeListener listener : listeners )
//                listener.newMasterBecameAvailable( masterId );
//        }
//        catch ( InterruptedException e )
//        {
//            Thread.interrupted();
//            throw new RuntimeException( e );
//        }
//    }

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
//        System.out.println( "got newMaster " + cause );
//        // TODO ehrmm...
//        if ( cause instanceof InformativeStackTrace )
//        {
//            if ( cause.getMessage().contains( "NodeDeleted" ) )
//            {
//                electMasterAndPingListeners();
//            }
//        }
    }

    @Override
    public void reconnect( Exception cause )
    {
        broker.restart();
    }
    
    @Override
    public void shutdown()
    {
        broker.shutdown();
    }
}
