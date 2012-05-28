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

package org.neo4j.kernel.ha2.protocol.cluster;

import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * TODO
 */
public class ClusterMembershipTest
    extends ClusterMockTest
{
    @Test
    public void threeNodesJoinAndThenLeave()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
             rounds( 70 ).
             join( 100, 1 ).
             join( 100,2 ).
             join( 100,3 ).
             leave( 100, 3 ).
             leave( 100, 2 ).
             leave( 100, 1 ));
    }

    @Test
    public void sevenNodesJoinAndThenLeave()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(7, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
              rounds( 112 ).
              join( 100, 1 ).
              join( 100,2 ).
              join( 100,3 ).
              join( 100,4 ).
              join( 100,5 ).
              join( 100,6 ).
              join( 100, 7 ).
              leave( 100, 7 ).
              leave( 100, 6 ).
              leave( 100, 5 ).
              leave( 100, 4 ).
              leave( 100, 3 ).
              leave( 100, 2 ).
              leave( 100, 1 )
         );
    }

    @Test
    public void oneNodeJoinThenTwoJoinRoughlyAtSameTime()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
              rounds( 500 ).
              join( 100, 1 ).
              join( 100, 2 ).
              join( 10, 3 ).
              message( 2000, "*** All are in " ).
              leave( 0, 3 )
         );
    }

    @Test
    public void oneNodeJoinThenThreeJoinRoughlyAtSameTime2()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(4, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
              rounds( 800 ).
              join( 100, 1 ).
              join( 100,2 ).
              join( 10, 3 ).
              join( 10, 4 ).
              message( 2000, "*** All are in " ).
              broadcast(10, 2, "Hello world").
              broadcast( 500, 2, "Hello world2" )
//              leave( 10, 4 ).
//              leave( 100, 3 ).
//              leave( 100, 2 ).
//              leave( 100, 1 )
         );
    }

    @Test
    public void twoNodesJoinThenOneLeavesAsThirdJoins()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
              join( 0, 1 ).
              join( 10, 2 ).
              message( 80, "*** 1 and 2 are in cluster" ).
              leave( 10, 2 ).
              join( 20, 3 ).
              verifyConfigurations( 1000L )
         );
    }

    // TODO this should cause server 2 to get a join exception
    @Ignore
    @Test
    public void oneNodeCreatesClusterAndThenAnotherJoinsAsFirstLeaves()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(2, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
              join( 0, 1 ).
              join( 10, 2 ).
              message( 0, "*** 1 and 2 are in cluster" ).
              leave( 20, 1 ).
              verifyConfigurations( 1000L )
         );
    }
}
