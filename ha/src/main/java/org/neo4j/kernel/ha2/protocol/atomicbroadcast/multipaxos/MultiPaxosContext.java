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

import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.helpers.collection.Iterables.toList;

import java.net.URI;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Specifications;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.timeout.Timeouts;

/**
 * Context shared by all Paxos state machines.
 */
public class MultiPaxosContext
{
    public ClusterContext clusterContext;

    public Timeouts timeouts;

    // Proposer/coordinator state
    Deque<Object> pendingValues = new LinkedList<Object>();
    Map<InstanceId,Object> bookedInstances = new HashMap<InstanceId,Object>(  );
    public long lastInstanceId = 0;
    ProposerInstanceStore proposerInstances = new ProposerInstanceStore();

    // Learner state
    List<LearnerInstance> learnerInstances = new ArrayList<LearnerInstance>(100);
    long lastReceivedInstanceId = -1;

    // Acceptor state
    AcceptorInstanceStore acceptorInstances = new InMemoryAcceptorInstanceStore();

    public MultiPaxosContext(ClusterContext clusterContext, Timeouts timeouts)
    {
        this.clusterContext = clusterContext;
        this.timeouts = timeouts;
    }

    public int getServerId()
    {
        return clusterContext.getConfiguration().getNodes().indexOf(clusterContext.getMe());
    }

    public Iterable<URI> getAcceptors()
    {
        return clusterContext.getConfiguration().getNodes();
    }

    public Iterable<URI> getLearners()
    {
        return clusterContext.getConfiguration().getNodes();
    }

    public URI getCoordinator()
    {
        return clusterContext.getConfiguration().getCoordinator();
    }

    public int getMinimumQuorumSize()
    {
        return clusterContext.getConfiguration().getNodes().size()-clusterContext.getConfiguration().getAllowedFailures();
    }
    
    public int getLearnerInstanceIndex( long instanceId )
    {
        return (int)(instanceId%learnerInstances.size());
    }

    public InstanceId newInstanceId()
    {
        return new InstanceId( lastInstanceId++);
    }
}
