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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.timeout.Timeouts;

import static org.neo4j.com_2.message.Message.*;

/**
 * TODO
 */
public class HeartbeatContext
{
    private ClusterContext context;
    List<URI> failed = new ArrayList<URI>(  );
    Iterable<HeartbeatListener> listeners = Listeners.newListeners();

    public HeartbeatContext(ClusterContext context)
    {
        this.context = context;
    }

    public void started()
    {
        failed.clear();
    }

    public void alive( final URI server )
    {
        if (failed.remove( server ))
            Listeners.notifyListeners( listeners, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.alive( server );
                }
            } );
    }

    public void failed( final URI server )
    {
        if (!failed.contains( server ))
        {
            failed.add( server );
            Listeners.notifyListeners( listeners, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.failed( server );
                }
            } );
        }
    }

    public List<URI> getFailed()
    {
        return failed;
    }

    public boolean isFailed(URI node)
    {
        return failed.contains( node );
    }

    public ClusterContext getClusterContext()
    {
        return context;
    }

    public void addHeartbeatListener( HeartbeatListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeHeartbeatListener(HeartbeatListener listener)
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    public void stopHeartbeatTimers()
    {
        // Cancel all existing timeouts
        for( URI server : context.getConfiguration().getNodes() )
        {
            context.timeouts.cancelTimeout( HeartbeatMessage.i_am_alive+"-"+server );
            context.timeouts.cancelTimeout( HeartbeatMessage.send_heartbeat+"-"+server );
        }
    }

    public void startHeartbeatTimers(Message<?> message)
    {
        // Start timers for sending and receiving heartbeats
        for( URI server : context.getConfiguration().getNodes() )
        {
            if (!context.isMe( server ))
            {
                context.timeouts.setTimeout( HeartbeatMessage.i_am_alive+"-"+server, timeout( HeartbeatMessage.timed_out, message, server ) );
                context.timeouts.setTimeout( HeartbeatMessage.send_heartbeat+"-"+server, timeout( HeartbeatMessage.send_heartbeat, message, server ) );
            }
        }
    }

    public void serverLeftCluster( URI server )
    {
        failed.remove( server );
    }
}
