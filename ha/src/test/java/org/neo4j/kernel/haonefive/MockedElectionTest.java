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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.BranchedDataPolicy;
import org.neo4j.kernel.haonefive.MockedDistributedElection.PreparedElection;
import org.neo4j.test.TargetDirectory;

@Ignore( "Not a test, but provides shared extendable functionality for other tests" )
public abstract class MockedElectionTest
{
    protected TargetDirectory PATH;
    protected HaOneFiveGraphDb[] dbs;
    protected MockedDistributedElection distributedElection;
    
    protected enum Types implements RelationshipType
    {
        TEST;
    }
    
    @Before
    public void before() throws Exception
    {
        PATH = TargetDirectory.forTest( getClass() );
        PATH.cleanup();
        distributedElection = new MockedDistributedElection();
    }
    
    @After
    public void after() throws Exception
    {
        for ( HaOneFiveGraphDb db : dbs )
            db.shutdown();
    }

    protected boolean hasBranch( int serverId )
    {
        File[] branches = BranchedDataPolicy.listBranchedDataDirectories( path( serverId ) );
        return branches != null && branches.length > 0;
    }

    protected void pullUpdates()
    {
        for ( HaOneFiveGraphDb db : dbs )
            db.pullUpdates();
    }

    protected void assertNodesExistsInAllDbs( String... expectedNames )
    {
        for ( int i = 0; i < dbs.length; i++ )
            assertNodesExists( i, expectedNames );
    }

    protected void electMaster( int id )
    {
        distributedElection.bluntlyForceMasterElection( id );
    }
    
    protected PreparedElection prepareMasterElection( int id )
    {
        return distributedElection.bluntlyPrepareMasterElection( id );
    }

    protected HaOneFiveGraphDb startDb( final int serverId )
    {
        HaOneFiveGraphDb db = new HaOneFiveGraphDb( path( serverId ), stringMap(
                "ha.server_id", "" + serverId ) )
        {
            @Override
            protected MasterElectionClient createMasterElectionClient()
            {
                MockedMasterElectionClient client = new MockedMasterElectionClient( distributedElection );
                distributedElection.addClient( client, serverId, 6361 );
                return client;
            }
        };
        dbs[serverId] = db;
        return db;
    }
    
    protected void startDbs( int count )
    {
        dbs = new HaOneFiveGraphDb[count];
        for ( int i = 0; i < count; i++ )
            startDb( i );
    }

    protected String path( int serverId )
    {
        return PATH.directory( "" + serverId, false ).getAbsolutePath();
    }

    protected void assertNodesExists( int id, String... names )
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

    protected void createNode( int id, String name )
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
