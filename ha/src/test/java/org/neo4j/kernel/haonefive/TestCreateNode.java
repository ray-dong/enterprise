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
import static org.neo4j.kernel.impl.util.FileUtils.copyRecursively;

import java.io.File;
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
    
    private enum Types implements RelationshipType
    {
        TEST;
    }
    
    @Before
    public void before() throws Exception
    {
        PATH = TargetDirectory.forTest( getClass() );
    }

    @After
    public void after() throws Exception
    {

    }
    
    @Test
    public void createHighlyAvailableNode() throws Exception
    {
        // Prepare dbs
        File db1Path = PATH.directory( "1", true );
        File db2Path = PATH.directory( "2", true );
        new HaOneFiveGraphDb( db1Path.getAbsolutePath(), stringMap( "ha.server_id", "1" ) ).shutdown();
        copyRecursively( db1Path, db2Path );
        
        // Start them
        HaOneFiveGraphDb db1 = new HaOneFiveGraphDb( db1Path.getAbsolutePath(), stringMap( "ha.server_id", "1" ) );
        HaOneFiveGraphDb db2 = new HaOneFiveGraphDb( db2Path.getAbsolutePath(), stringMap( "ha.server_id", "2" ) );
        
        // Fake master election
        db1.newMasterElected( "http://localhost:6361", 1 );
        db2.newMasterElected( "http://localhost:6361", 1 );
        db1.masterChanged( "http://localhost:6361" );
        db1.masterChanged( "http://localhost:6361" );
        
        // Create node on master, then on slave
        createNode( db1, "yo" );
        createNode( db2, "ya" );
        
        // Verify that both nodes are in both dbs
        for ( HaOneFiveGraphDb db : new HaOneFiveGraphDb[] { db1, db2 } )
            assertNodesExists( db, "yo", "ya" );
        
        // Shut down
        db1.shutdown();
        db2.shutdown();
    }

    private void assertNodesExists( HaOneFiveGraphDb db, String... names )
    {
        Set<String> expectation = new HashSet<String>( asList( names ) );
        for ( Relationship rel : db.getReferenceNode().getRelationships() )
            assertTrue( expectation.remove( rel.getEndNode().getProperty( "name" ) ) );
        assertTrue( expectation.isEmpty() );
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
