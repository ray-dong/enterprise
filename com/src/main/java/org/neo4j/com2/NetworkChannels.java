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
package org.neo4j.com2;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channel;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class NetworkChannels
{
    public interface NetworkChannelsListener
    {
        void listeningAt(URI me);
        void channelOpened(URI to);
        void channelClosed(URI to);
    }
    
    private StringLogger msgLog;

    public interface ChannelFactory
    {
        Channel openChannel(URI uri);
    }
    
    private URI me;

    private Map<URI, Channel> connections = new ConcurrentHashMap<URI, Channel>();

    private Iterable<NetworkChannelsListener> listeners = Listeners.newListeners();

    public NetworkChannels(StringLogger msgLog)
    {
        this.msgLog = msgLog;
    }

    public void listeningAt(URI me)
    {
        this.me = me;
        for( NetworkChannelsListener listener : listeners )
        {
            listener.listeningAt( me );
        }
    }

    public void broadcast(Object message, ChannelFactory channelFactory)
    {
        for (int i = 1234; i < 1234+2; i++)
        {
            URI uri;
            try
            {
                uri = new URI("neo4j://127.0.0.1:"+i);
            } catch (URISyntaxException e)
            {
                e.printStackTrace();
                continue;
            }

            if (!uri.equals(me))
            {
                send(uri, message, channelFactory);
            }
        }
    }

    public void send(URI to, Object message, ChannelFactory factory)
    {
        Channel channel = getChannel(to);

        try
        {
            if (channel == null)
            {
                channel = factory.openChannel(to);
                openedChannel(to, channel);
            }
        } catch (Exception e)
        {
            msgLog.logMessage("Could not connect to:" + to);
            return;
        }

        try
        {
            msgLog.logMessage("Sending to "+to+": "+message);
            channel.write(message);
        } catch (Exception e)
        {
            e.printStackTrace();
            channel.close();
            closedChannel(to);
        }
    }

    public void openedChannel(URI uri, Channel ctxChannel)
    {
        connections.put(uri, ctxChannel);

        for( NetworkChannelsListener listener : listeners )
        {
            listener.channelOpened( uri );
        }
    }

    public void closedChannel(URI uri)
    {
        Channel channel = connections.remove(uri);
        if (channel != null)
            channel.close();

        for( NetworkChannelsListener listener : listeners )
        {
            listener.channelClosed( uri );
        }
    }

    public URI getMe()
    {
        return me;
    }

    public Channel getChannel(URI uri)
    {
        return connections.get(uri);
    }
    
    public void addNetworkChannelsListener(NetworkChannelsListener listener)
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeNetworkChannelsListener(NetworkChannelsListener listener)
    {
        listeners = Listeners.removeListener( listener, listeners );
    }
}
