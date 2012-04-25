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
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestCreateNode
{
    private LocalhostZooKeeperCluster zoo;
    private TargetDirectory PATH;
    private HaOneFiveGraphDb[] dbs;
    
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
    public void createHighlyAvailableNode() throws Exception
    {
        // Start them
        startDbs( 2 );
        
        electNewMaster();
        
        createNode( dbs[0], "yo" ); // master
        createNode( dbs[1], "ya" ); // slave
        
        // Verify that all nodes are in both dbs
        for ( HaOneFiveGraphDb db : dbs )
            assertNodesExists( db, "yo", "ya" );
        
        shutdownDb( 0 );
        electNewMaster();
        startDb( 0 );

        // Create node on master, then on slave
        createNode( dbs[1], "ye" ); // master
        createNode( dbs[0], "yi" ); // slave
        
        // Verify that all nodes are in both dbs
        for ( HaOneFiveGraphDb db : dbs )
            assertNodesExists( db, "yo", "ya", "yi", "ye" );

        createNode( dbs[1], "yu" ); // master
        dbs[0].pullUpdates();
        
        for ( HaOneFiveGraphDb db : dbs )
            assertNodesExists( db, "yo", "ya", "yi", "ye", "yu" );
    }

    private void shutdownDb( int i )
    {
        dbs[i].shutdown();
        dbs[i] = null;
    }

    private HaOneFiveGraphDb startDb( int serverId )
    {
        HaOneFiveGraphDb db = new HaOneFiveGraphDb( path( serverId ), stringMap(
                "ha.server_id", "" + serverId,
                "ha.coordinators", zoo.getConnectionString() ) );
        dbs[serverId] = db;
        registerListeners();
        return db;
    }
    
    private void startDbs( int count )
    {
        dbs = new HaOneFiveGraphDb[count];
        for ( int i = 0; i < count; i++ )
            startDb( i );
    }
    
    private void registerListeners()
    {
        for ( HaOneFiveGraphDb to : dbs )
        {
            if ( to == null )
                continue;
            to.masterElectionClient.clearListeners();
            for ( HaOneFiveGraphDb db : dbs )
                if ( db != null )
                    to.masterElectionClient.addListener( db );
        }
    }

    @Test
    public void slaveBecomesAwareOfChangedMaster() throws Exception
    {
        startDbs( 3 );
        electNewMaster();
        
        // do something on master and verify that pull will get it
        electNewMaster();
        // do something on master and verify that pull will get it
    }

    private String path( int serverId )
    {
        return PATH.directory( "" + serverId, false ).getAbsolutePath();
    }

    private void electNewMaster()
    {
        for ( HaOneFiveGraphDb db : dbs )
        {
            if ( db != null )
            {
                db.masterElectionClient.bluntlyForceMasterElection();
                break;
            }
        }
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
