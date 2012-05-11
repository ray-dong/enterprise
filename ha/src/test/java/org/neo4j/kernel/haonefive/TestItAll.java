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

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestItAll
{
    private TargetDirectory PATH;
    private HaOneFiveGraphDb[] dbs;
    private LocalhostZooKeeperCluster zoo;

    private enum Types implements RelationshipType
    {
        TEST;
    }
    
    @Before
    public void before() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        PATH = TargetDirectory.forTest( getClass() );
        PATH.cleanup();
    }
    
    @After
    public void after() throws Exception
    {
        for ( HaOneFiveGraphDb db : dbs )
            db.shutdown();
    }
    
    @Test
    public void slaveCreateNode() throws Exception
    {
        startDbs( 2 );
        int master = getMaster();
        createNode( (master+1)%2, "Slave node" );
        assertNodesExists( 0, "Slave node" );
        assertNodesExists( 1, "Slave node" );
    }

    private void pullUpdates()
    {
        for ( HaOneFiveGraphDb db : dbs )
            db.pullUpdates();
    }
    
    private void startDbs( int count )
    {
        dbs = new HaOneFiveGraphDb[count];
        for ( int i = 0; i < count; i++ )
            startDb( i );
    }
    
    private HaOneFiveGraphDb startDb( final int serverId )
    {
        HaOneFiveGraphDb db = new HaOneFiveGraphDb( path( serverId ), stringMap(
                "ha.server_id", "" + serverId,
                "ha.coordinators", zoo.getConnectionString() ) );
        dbs[serverId] = db;
        return db;
    }
    
    private String path( int serverId )
    {
        return PATH.directory( "" + serverId, false ).getAbsolutePath();
    }
    
    private void createNode( int id, String name )
    {
        GraphDatabaseService db = dbs[id];
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        db.getReferenceNode().createRelationshipTo( node, Types.TEST );
        node.setProperty( "name", name );
        tx.success();
        tx.finish();
    }
    
    private int getMaster()
    {
        for ( int i = 0; i < dbs.length; i++ )
            if ( dbs[i].isMaster() )
                return i;
        
        fail( "No master" );
        return -1; // Unreachable code, fail will throw exception
    }

    private void assertNodesExists( int id, String... names )
    {
        GraphDatabaseService db = dbs[id];
        Set<String> expectation = new HashSet<String>( asList( names ) );
        for ( Relationship rel : db.getReferenceNode().getRelationships() )
        {
            String name = (String) rel.getEndNode().getProperty( "name" );
            assertTrue( "Found unexpected name " + name, expectation.remove( name ) );
        }
        assertTrue( "Expected entries not encountered: " + expectation, expectation.isEmpty() );
    }
}
