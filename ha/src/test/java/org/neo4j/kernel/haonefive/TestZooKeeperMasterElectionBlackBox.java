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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZooKeeperMasterElectionBlackBox
{
    private Instance[] instances;
    
    @Before
    public void before() throws Exception
    {
        instances = new Instance[3];
        for ( int i = 0; i < instances.length; i++ )
            instances[i] = new Instance( new ZooKeeperMasterElectionClient( null, null, null, null ), null );
    }

    @After
    public void after() throws Exception
    {
        for ( Instance instance : instances )
            instance.shutdown();
    }
    
    @Test
    public void shouldElectRightMaster() throws Exception
    {
        
    }
    
    @Test
    public void shouldReelectMasterWhenCurrentMasterIsRemoved() throws Exception
    {
        
    }
    
    private static class Instance
    {
        private final MasterElectionClient client;
        private final MasterChangeListener listener;
        
        Instance( MasterElectionClient client, MasterChangeListener listener )
        {
            this.client = client;
            this.listener = listener;
        }

        public void shutdown()
        {
            client.shutdown();
        }
    }
}
