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

import org.junit.Test;
import org.neo4j.kernel.haonefive.MockedDistributedElection.PreparedElection;

public class TestStateSwitchBlock extends MockedElectionTest
{
    @Test
    public void incomingTransactionsShouldBlockWhenStateSwitchingBlockadeIsEntered() throws Exception
    {
        startDbs( 2 );
        PreparedElection election = prepareMasterElection( 1 );
        // Try to do a write transaction in another thread or something and make sure it blocks
        election.complete();
        // Verify that the transaction can complete now
    }
    
    @Test
    public void activeWriteTransactionsShouldFailAfterMasterSwitch() throws Exception
    {
        startDbs( 2 );
        // Begin transaction and create a node for example, but keep tx open
        electMaster( 1 );
        // Continue transaction and verify that it fails
    }
    
    @Test
    public void readTransactionsCanCarryOnThroughStateSwitch() throws Exception
    {
        startDbs( 2 );
        PreparedElection election = prepareMasterElection( 1 );
        // Try to do a read transaction in another thread or something and make sure it can complete
        election.complete();
    }
}
