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

public class TestCreateNode
{
    private TargetDirectory PATH;
    private MockedMasterElectionClient masterElection;
    private HaOneFiveGraphDb[] dbs;
    
    private enum Types implements RelationshipType
    {
        TEST;
    }
    
    @Before
    public void before() throws Exception
    {
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
    public void createNodesWhenIdDifferentRoles() throws Exception
    {
        startDbs( 2 );
        electMaster( 0 );
        
        createNode( 0, "yo" );
        createNode( 1, "ya" );
        assertNodesExistsInAllDbs( "yo", "ya" );
        
        electMaster( 1 );
        
        createNode( 1, "ye" );
        assertNodesExists( 1, "yo", "ya", "ye" );
        assertNodesExists( 0, "yo", "ya" );
        createNode( 0, "yi" );
        assertNodesExistsInAllDbs( "yo", "ya", "ye", "yi" );
    }
    
    @Test
    public void changeToAnotherMaster() throws Exception
    {
        startDbs( 3 );
        electMaster( 0 );
        createNode( 1, "first" );
        assertNodesExists( 0, "first" );
        assertNodesExists( 1, "first" );
        assertNodesExists( 2 );
        pullUpdates();
        
        electMaster( 2 );
        createNode( 1, "second" );
        assertNodesExists( 0, "first" );
        assertNodesExists( 1, "first", "second" );
        assertNodesExists( 2, "first", "second" );
    }

    private void pullUpdates()
    {
        for ( HaOneFiveGraphDb db : dbs )
            db.pullUpdates();
    }

    private void assertNodesExistsInAllDbs( String... expectedNames )
    {
        for ( int i = 0; i < dbs.length; i++ )
            assertNodesExists( i, expectedNames );
    }

    private void electMaster( int id )
    {
        masterElection.bluntlyForceMasterElection( id );
    }

    private void shutdownDb( int id )
    {
        dbs[id].shutdown();
        dbs[id] = null;
        masterElection.removeListener( id );
    }

    private HaOneFiveGraphDb startDb( int serverId )
    {
        HaOneFiveGraphDb db = new HaOneFiveGraphDb( path( serverId ), stringMap(
                "ha.server_id", "" + serverId ) )
        {
            @Override
            protected MasterElectionClient createMasterElectionClient()
            {
                return masterElection;
            }
        };
        dbs[serverId] = db;
        masterElection.addListener( db, serverId, 6361 );
        return db;
    }
    
    private void startDbs( int count )
    {
        masterElection = new MockedMasterElectionClient();
        dbs = new HaOneFiveGraphDb[count];
        for ( int i = 0; i < count; i++ )
            startDb( i );
    }

    private String path( int serverId )
    {
        return PATH.directory( "" + serverId, false ).getAbsolutePath();
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
}
