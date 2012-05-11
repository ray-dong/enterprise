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

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class TestRoleSwitching extends MockedElectionTest
{
    @Test
    public void createNodesWhenInDifferentRoles() throws Exception
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
    public void changeToAnotherMasterWhileStayingSlave() throws Exception
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
    
    @Test
    public void detectBranchWhenSteppingDownFromMasterRoleHavingUndistributedTransactions() throws Exception
    {
        startDbs( 3 );
        electMaster( 0 );
        createNode( 0, "first" );
        electMaster( 1 );
        assertTrue( hasBranch( 0 ) );
        assertFalse( hasBranch( 1 ) );
        assertFalse( hasBranch( 2 ) );
    }
}
