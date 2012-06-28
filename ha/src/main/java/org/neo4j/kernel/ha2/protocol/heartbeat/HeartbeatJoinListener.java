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

package org.neo4j.kernel.ha2.protocol.heartbeat;

import java.net.URI;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterAdapter;

/**
 * When a node joins a cluster, setup a heartbeat for it
 */
public class HeartbeatJoinListener
    extends ClusterAdapter
{
    private MessageProcessor outgoing;

    public HeartbeatJoinListener( MessageProcessor outgoing )
    {
        this.outgoing = outgoing;
    }

    @Override
    public void joinedCluster( URI node )
    {
        outgoing.process( Message.internal(HeartbeatMessage.reset_send_heartbeat, node) );
    }
}
