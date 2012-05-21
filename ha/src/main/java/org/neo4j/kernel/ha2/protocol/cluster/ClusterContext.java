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

import java.net.URI;
import java.util.List;

import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.ha2.timeout.Timeouts;

/**
 *
 * Context for cluster API state machine
 */
public class ClusterContext
{
    public URI me;
    public Iterable<ClusterListener> listeners = Listeners.newListeners();
    public final ClusterConfiguration configuration;
    public final Timeouts timeouts;

    public ClusterContext(ClusterConfiguration configuration, Timeouts timeouts)
    {
        this.configuration = configuration;
        this.timeouts = timeouts;
    }

    public void create()
    {
        configuration.joined(me);
        Listeners.notifyListeners(listeners, new Listeners.Notification<org.neo4j.kernel.ha2.protocol.cluster.ClusterListener>()
        {
            @Override
            public void notify(ClusterListener listener)
            {
                listener.notifyClusterChange(configuration);
            }
        });
    }

    public void joined(List<URI> nodes)
    {
        configuration.setNodes( nodes );
        Listeners.notifyListeners(listeners, new Listeners.Notification<org.neo4j.kernel.ha2.protocol.cluster.ClusterListener>()
        {
            @Override
            public void notify(ClusterListener listener)
            {
                listener.notifyClusterChange(configuration);
            }
        });
    }

    public void setMe(URI me)
    {
        this.me = me;
    }

    public URI getMe()
    {
        return me;
    }

    public ClusterConfiguration getConfiguration()
    {
        return configuration;
    }

    public void addClusterListener(ClusterListener listener)
    {
        listeners = Listeners.addListener(listener, listeners);
    }

    public void removeClusterListener(ClusterListener listener)
    {
        listeners = Listeners.removeListener(listener, listeners);
    }

    public boolean isCoordinator()
    {
        return configuration.getCoordinator().equals(me);
    }
}
