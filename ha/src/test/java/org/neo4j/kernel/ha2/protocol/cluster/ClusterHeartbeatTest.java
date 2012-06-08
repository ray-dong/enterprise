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
import org.junit.Test;

/**
 * TODO
 */
public class ClusterHeartbeatTest
    extends ClusterMockTest
{
    @Test
    public void threeNodesJoinAndNoFailures()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
             rounds( 200 ).
             join( 100, 1 ).
             join( 100,2 ).
             join( 100,3 ).
             leave(1000, 1).
             leave(200, 2).
             leave(200, 3));
    }

    @Test
    public void threeNodesJoinAndThenOneDies()
        throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
         testCluster(3, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
             rounds( 250 ).
             join( 100, 1 ).
             join( 100,2 ).
             join( 100,3 ).
             down( 500, 3).
             message( 1000, "*** Should have seen failure by now" ).
             up(0, 3).
             message( 200, "*** Should have recovered by now" ).
             leave(200, 1).
             leave(200, 2).
             leave(200, 3));
    }
}