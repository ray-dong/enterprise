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

import java.io.Serializable;
import java.net.URI;
import java.util.List;

import org.neo4j.com_2.message.MessageType;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.InstanceId;

/**
 * Messages to implement the Cluster API state machine
 */
public enum ClusterMessage
    implements MessageType
{
    // Method messages
    create,createResponse,join,joinResponse,leave,leaveResponse,

    addClusterListener, removeClusterListener,

    // Protocol messages
    configuration,configurationResponse,configurationTimeout, configurationChanged, joinFailed;

    public static class ConfigurationResponseState
        implements Serializable
    {
        private List<URI> acceptors;
        private List<URI> nodes;
        private InstanceId latestReceivedInstanceId;

        public ConfigurationResponseState(List<URI> acceptors, List<URI> nodes, InstanceId latestReceivedInstanceId )
        {
            this.acceptors = acceptors;
            this.nodes = nodes;
            this.latestReceivedInstanceId = latestReceivedInstanceId;
        }

        public List<URI> getAcceptors()
        {
            return acceptors;
        }

        public List<URI> getNodes()
        {
            return nodes;
        }

        public InstanceId getLatestReceivedInstanceId()
        {
            return latestReceivedInstanceId;
        }
    }

    public static class ConfigurationChangeState
        implements Serializable
    {
        private URI join;
        private URI leave;

        public void join(URI uri)
        {
            this.join = uri;
        }

        public void leave(URI uri)
        {
            this.leave = uri;
        }

        public URI getJoin()
        {
            return join;
        }

        public URI getLeave()
        {
            return leave;
        }

        public void apply(ClusterConfiguration config)
        {
            if (join != null)
                config.joined( join );

            if (leave != null)
                config.left( leave );
        }

        public boolean isLeaving( URI me )
        {
            return me.equals(leave);
        }

        @Override
        public String toString()
        {
            return "Change cluster config: "+(join == null ? "leave:"+leave : "join:"+join.toString());
        }
    }

}
