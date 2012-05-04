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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos;

import org.neo4j.kernel.ha2.MultiPaxosServer;
import org.neo4j.kernel.ha2.NetworkFailureStrategy;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.TestProtocolServer;

/**
 * TODO
 */
public class MultiPaxosNetworkMock
    extends NetworkMock<PaxosContext, AtomicBroadcastMessage, MultiPaxosServer>
{
    public MultiPaxosNetworkMock()
    {
    }

    public MultiPaxosNetworkMock( NetworkFailureStrategy failureStrategy )
    {
        super( failureStrategy );
    }

    @Override
    protected TestProtocolServer<PaxosContext, AtomicBroadcastMessage, MultiPaxosServer> newTestProtocolServer( String serverId )
    {
        return new TestProtocolServer<PaxosContext, AtomicBroadcastMessage, MultiPaxosServer>( serverId )
        {
            @Override
            protected void init()
            {
                context = new PaxosContext();
                server = new MultiPaxosServer( context, receiver, sender, new TestFailureHandlerFactory() );
            }
        };
    }
}
