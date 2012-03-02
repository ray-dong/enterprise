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

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class NetworkMessageReceiver
    implements Lifecycle
{

    private ExecutorService executor;
    private ServerBootstrap bootstrap;
    private Channel channel;

    public interface Configuration
    {
        int port(int def);
    }
    
    private Configuration config;
    private StringLogger msgLog;
    private NetworkChannels channels;

    private List<NetworkMessageListener> listeners = new ArrayList<NetworkMessageListener>();

    public NetworkMessageReceiver(Configuration config, StringLogger msgLog, NetworkChannels channels)
    {
        this.config = config;
        this.msgLog = msgLog;
        this.channels = channels;
    }

    @Override
    public void init() throws Throwable
    {
        executor = Executors.newCachedThreadPool();

        ServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(
                executor, executor, 3 );
        bootstrap = new ServerBootstrap( channelFactory );
        bootstrap.setPipelineFactory(new NetworkNodePipelineFactory());

        int port = config.port(1234);
        int checkPort = port;
        while (true)
        {
            try
            {
                channel = bootstrap.bind(new InetSocketAddress("127.0.0.1", checkPort));
                channels.listeningAt((getURI((InetSocketAddress) channel.getLocalAddress())));
                break;
            }
            catch ( ChannelException e )
            {
                if (checkPort < port+3)
                {
                    checkPort++;
                    continue;
                }

                msgLog.logMessage( "Failed to bind master server to port " + port, e );
                executor.shutdown();
                throw e;
            }
        }

        ChannelGroup channelGroup = new DefaultChannelGroup();
        channelGroup.add(channel);
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
        executor.shutdownNow();
        channel.disconnect();
    }

    public void addMessageListener(NetworkMessageListener listener)
    {
        listeners.add(listener);
    }

    private URI getURI(InetSocketAddress address) throws URISyntaxException
    {
        return new URI("neo4j:/" + address);
    }

    private class NetworkNodePipelineFactory
        implements ChannelPipelineFactory
    {
        @Override
        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addFirst("log", new LoggingHandler());
            addSerialization(pipeline, 1024 * 1000);
            pipeline.addLast( "serverHandler", new MessageReceiver() );
            return pipeline;
        }

        private void addSerialization(ChannelPipeline pipeline, int frameLength)
        {
            pipeline.addLast( "frameDecoder",
                    new ObjectDecoder(1024*1000, getClass().getClassLoader() ) );
            pipeline.addLast( "frameEncoder", new ObjectEncoder());
        }
    }
    
    private class MessageReceiver
            extends SimpleChannelHandler
    {
        @Override
        public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
        {
            Channel ctxChannel = ctx.getChannel();
            channels.openedChannel(getURI((InetSocketAddress) ctxChannel.getRemoteAddress()), ctxChannel);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception
        {
            final Object message =  event.getMessage();
            System.out.println("Received:" + message);
            executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    for (NetworkMessageListener listener : listeners)
                    {
                        listener.received(message);
                    }
                }
            });
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
        {
            channels.closedChannel(getURI((InetSocketAddress) ctx.getChannel().getRemoteAddress()));
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
        {
            channels.closedChannel(getURI((InetSocketAddress) ctx.getChannel().getRemoteAddress()));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
        {
            System.out.println("Receive exception:");
            e.getCause().printStackTrace();
        }

        @Override
        public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception
        {
            System.out.println("Write complete");
            super.writeComplete(ctx, e);
        }
    }
}
