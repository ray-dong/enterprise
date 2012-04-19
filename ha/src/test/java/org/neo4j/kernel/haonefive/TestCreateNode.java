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

import java.io.File;
import java.util.HashSet;
import java.util.Set;

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
    
    private enum Types implements RelationshipType
    {
        TEST;
    }
    
    @Before
    public void before() throws Exception
    {
        PATH = TargetDirectory.forTest( getClass() ).cleanup();
    }
    
    @Test
    public void createHighlyAvailableNode() throws Exception
    {
        // Start them
        File db1Path = path( 1 );
        File db2Path = path( 2 );
        HaOneFiveGraphDb db1 = new HaOneFiveGraphDb( db1Path.getAbsolutePath(), stringMap( "ha.server_id", "1" ) );
        HaOneFiveGraphDb db2 = new HaOneFiveGraphDb( db2Path.getAbsolutePath(), stringMap( "ha.server_id", "2" ) );
        
        // Elect 1 as master
        electNewMaster( 1, db1, db2 );
        
        createNode( db1, "yo" ); // master
        createNode( db2, "ya" ); // slave
        
        // Verify that all nodes are in both dbs
        for ( HaOneFiveGraphDb db : new HaOneFiveGraphDb[] { db1, db2 } )
            assertNodesExists( db, "yo", "ya" );
        
        // Elect 2 as new master
        electNewMaster( 2, db1, db2 );

        // Create node on master, then on slave
        createNode( db2, "ye" ); // master
        createNode( db1, "yi" ); // slave
        
        // Verify that all nodes are in both dbs
        for ( HaOneFiveGraphDb db : new HaOneFiveGraphDb[] { db1, db2 } )
            assertNodesExists( db, "yo", "ya", "yi", "ye" );

        createNode( db2, "yu" ); // master
        db1.pullUpdates();
        
        for ( HaOneFiveGraphDb db : new HaOneFiveGraphDb[] { db1, db2 } )
            assertNodesExists( db, "yo", "ya", "yi", "ye", "yu" );
        
        // Shut down
        db1.shutdown();
        db2.shutdown();
    }

    private File path( int serverId )
    {
        return PATH.directory( "" + serverId, false );
    }

    private void electNewMaster( int id, HaOneFiveGraphDb... dbsToTell )
    {
        for ( HaOneFiveGraphDb db : dbsToTell )
            db.newMasterElected( "http://localhost:" + (6361 + id), id );
        for ( HaOneFiveGraphDb db : dbsToTell )
            db.masterChanged( "http://localhost:" + (6361 + id) );
    }

    private void assertNodesExists( HaOneFiveGraphDb db, String... names )
    {
        Set<String> expectation = new HashSet<String>( asList( names ) );
        for ( Relationship rel : db.getReferenceNode().getRelationships() )
        {
            String name = (String) rel.getEndNode().getProperty( "name" );
            assertTrue( "Found unexpected name " + name, expectation.remove( name ) );
        }
        assertTrue( "Expected entries not encountered: " + expectation, expectation.isEmpty() );
    }

    private void createNode( GraphDatabaseService db, String name )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        db.getReferenceNode().createRelationshipTo( node, Types.TEST );
        node.setProperty( "name", name );
        tx.success();
        tx.finish();
    }
}
