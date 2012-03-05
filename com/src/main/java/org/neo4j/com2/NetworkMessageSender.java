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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.queue.BlockingReadHandler;
import org.neo4j.com.ComException;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class NetworkMessageSender
    implements Lifecycle
{
    StringLogger msgLog;
    private NetworkChannels channels;
    private ClientBootstrap bootstrap;
    private NetworkChannels.ChannelFactory channelFactory;
    private final ExecutorService executor;

    public NetworkMessageSender(StringLogger msgLog, NetworkChannels channels)
    {
        this.msgLog = msgLog;
        this.channels = channels;

        channelFactory = new DefaultChannelFactory();
        executor = Executors.newCachedThreadPool();
    }

    @Override
    public void init() throws Throwable
    {
        bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(executor, executor) );
        bootstrap.setPipelineFactory(new NetworkNodePipelineFactory());
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
        executor.shutdown();
    }
    
    public void send(String to, Object message)
    {
        URI uri = null;
        try
        {
            uri = new URI(to);
        } catch (URISyntaxException e)
        {
            msgLog.logMessage("Invalid URI:"+to);
            return;
        }

        channels.send(uri, message, channelFactory);
    }

    public void broadcast(Object message)
    {
        channels.broadcast(message, channelFactory);
    }

    private class NetworkNodePipelineFactory
            implements ChannelPipelineFactory
    {
        @Override
        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();
            addSerialization(pipeline);

            BlockingReadHandler<ChannelBuffer> reader = new BlockingReadHandler<ChannelBuffer>(
                    new ArrayBlockingQueue<ChannelEvent>( 3, false ) );
            pipeline.addLast("blockingHandler", reader);

            return pipeline;
        }

        private void addSerialization(ChannelPipeline pipeline)
        {
            pipeline.addLast( "frameDecoder",
                    new ObjectDecoder( 1024*1000, getClass().getClassLoader()) );
            pipeline.addLast( "frameEncoder", new ObjectEncoder());
        }
    }

    private class DefaultChannelFactory implements NetworkChannels.ChannelFactory
    {
        @Override
        public Channel openChannel(URI neo4jUri)
        {
            SocketAddress address = new InetSocketAddress(neo4jUri.getHost(), neo4jUri.getPort());

            ChannelFuture channelFuture = bootstrap.connect( address );
//            channelFuture.awaitUninterruptibly( 5, TimeUnit.SECONDS );

            try
            {
                if ( channelFuture.await(5, TimeUnit.SECONDS) && channelFuture.getChannel().isConnected())
                {
                    msgLog.logMessage( "Opened a new channel to " + address, true );
                    return channelFuture.getChannel();
                }

                String msg = "Client could not connect to " + address;
                msgLog.logMessage( msg, true );
                throw new ComException(msg);
            }
            catch ( InterruptedException e )
            {
                msgLog.logMessage( "Interrupted", e );
                throw new ComException(e);
            }
        }
    }
}
