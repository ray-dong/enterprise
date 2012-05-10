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
import org.neo4j.kernel.ha2.timeout.Timeouts;

import static org.neo4j.helpers.collection.Iterables.*;

/**
 * Context shared by all Paxos state machines.
 */
public class MultiPaxosContext
{
    public Timeouts timeouts;

    private String me;

    private List<String> possibleServers = new ArrayList<String>(  );
    private Set<String> failedServers = new HashSet<String>(  );

    Iterable<AtomicBroadcastListener> listeners = Listeners.newListeners();

    ClusterConfiguration clusterConfiguration;

    // Proposer/coordinator state
    Deque<Object> pendingValues = new LinkedList<Object>();
    Map<InstanceId,Object> bookedInstances = new HashMap<InstanceId,Object>(  );
    public long lastInstanceId = 0;
    ProposerInstanceStore proposerInstances = new ProposerInstanceStore();

    // Learner state
    List<LearnerInstance> learnerInstances = new ArrayList<LearnerInstance>(100);
    long lastLearnedInstanceId = -1;

    // Acceptor state
    AcceptorInstanceStore acceptorInstances = new InMemoryAcceptorInstanceStore();

    public void addAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    public void setMe( String me )
    {
        this.me = me;
    }

    public String getMe()
    {
        return me;
    }

    public int getServerId()
    {
        return clusterConfiguration.getProposers().indexOf( me );
    }

    public void setPossibleServers( String... serverIds )
    {
        possibleServers.clear();
        possibleServers.addAll( toList( iterable( serverIds ) ) );
    }

    public Iterable<String> getPossibleServers()
    {
        return possibleServers;
    }

    public Iterable<String> getLiveServers()
    {
        return Iterables.filter( Specifications.in( failedServers ), possibleServers );
    }

    public void fail(String serverId)
    {
        failedServers.add( serverId );
    }

    public void recover(String serverId)
    {
        failedServers.remove( serverId );
    }

    public Iterable<String> getAcceptors()
    {
        return clusterConfiguration.getAcceptors();
    }

    public Iterable<String> getLearners()
    {
        return clusterConfiguration.getLearners();
    }

    public String getCoordinator()
    {
        return clusterConfiguration.getCoordinator();
    }

    public int getMinimumQuorumSize()
    {
        return clusterConfiguration.getAcceptors().size()/2+1;
    }
    
    public void learnValue( final Object value )
    {
        Listeners.notifyListeners( listeners, new Listeners.Notification<AtomicBroadcastListener>()
                {
                    @Override
                    public void notify( AtomicBroadcastListener listener )
                    {
                        listener.receive( value );
                    }
                } );
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
