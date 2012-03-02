/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.com2;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.queue.BlockingReadHandler;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO
 */
public class NetworkChannels
{
    public interface ChannelFactory
    {
        Channel openChannel(URI uri);
    }
    
    private URI me;

    private Map<URI, Channel> connections = new ConcurrentHashMap<URI, Channel>();

    public NetworkChannels()
    {
    }

    public void listeningAt(URI me)
    {
        this.me = me;
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
            System.out.println("Could not connect to:" + to);
            return;
        }

        try
        {
            System.out.println("Sending to "+to+": "+message);
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
    }

    public void closedChannel(URI uri)
    {
        Channel channel = connections.remove(uri);
        if (channel != null)
            channel.close();
    }

    public URI getMe()
    {
        return me;
    }

    public Channel getChannel(URI uri)
    {
        return connections.get(uri);
    }
    
    
}
