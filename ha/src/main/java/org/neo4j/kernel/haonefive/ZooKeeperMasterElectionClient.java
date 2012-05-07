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

import static org.neo4j.kernel.haonefive.UrlUtil.toUrl;

import java.net.URL;

import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.InformativeStackTrace;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterEventReceiver;
import org.neo4j.kernel.ha.SlaveDatabaseOperations;
import org.neo4j.kernel.ha.shell.ZooClientFactory;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperMachine;
import org.neo4j.kernel.impl.util.StringLogger;

public class ZooKeeperMasterElectionClient extends AbstractMasterElectionClient
        implements ZooClientFactory, SlaveDatabaseOperations, ClusterEventReceiver
{
    private final Broker broker;
    private final Config config;
    private final HaServiceSupplier stuff;
    private final StoreIdGetter storeIdGetter;
    private final String storeDir;
    private Machine currentMaster = ZooKeeperMachine.NO_MACHINE;
    
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
        if ( currentMaster.getMachineId() == -1 )
            if ( !figureOutCurrentMaster() )
                return;
        pingListenersAboutCurrentMaster();
    }
    
    private boolean figureOutCurrentMaster()
    {
        Machine master = broker.getMasterReally( true ).other();
        if ( master.getMachineId() != currentMaster.getMachineId() )
        {
            currentMaster = master;
            return true;
        }
        return false;
    }

    private void pingListenersAboutCurrentMaster()
    {
        URL masterUrl = toUrl( currentMaster.getServer().first(), currentMaster.getServer().other() );
        int masterServerId = currentMaster.getMachineId();
        for ( MasterChangeListener listener : listeners )
            listener.newMasterElected( masterUrl, masterServerId );
        for ( MasterChangeListener listener : listeners )
            listener.newMasterBecameAvailable( masterUrl);
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
    public void newMaster( Exception cause )
    {
        cause.printStackTrace();
        if ( cause instanceof InformativeStackTrace )
        {
            if ( cause.getMessage().contains( "NodeDeleted" ) )
            {
                broker.callForData();
                sleep( 2000 ); // TODO Ehurm
            }
        }
        
        if ( figureOutCurrentMaster() )
            pingListenersAboutCurrentMaster();
    }
    
    private void sleep( int i )
    {
        try
        {
            Thread.sleep( i );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public MasterElectionInput askForMasterElectionInput()
    {
        Pair<Long, Integer> lastTx = getLastTxData();
        return new MasterElectionInput( lastTx.first(), lastTx.other() );
    }
    
    @Override
    public Pair<Long, Integer> getLastTxData()
    {
        Triplet<Long, Integer, Long> data = stuff.getLastTx();
        return Pair.of( data.first(), data.second() );
    }

    @Override
    public void reconnect( Exception cause )
    {
        broker.restart();
    }
    
    public void shutdown()
    {
        broker.shutdown();
    }
}
