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

package org.neo4j.kernel.ha2;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageType;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.InMemoryAcceptorInstanceStore;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ServerIdElectionCredentialsProvider;
import org.neo4j.kernel.ha2.statemachine.StateTransitionLogger;
import org.neo4j.kernel.ha2.timeout.MessageTimeoutStrategy;
import org.slf4j.LoggerFactory;

/**
 * This mocks message delivery, message loss, and time for timeouts and message latency
 * between protocol servers.
 */
public class NetworkMock
{
    Map<String, TestProtocolServer> participants = new HashMap<String, TestProtocolServer>();

    private List<MessageDelivery> messageDeliveries = new ArrayList<MessageDelivery>();

    private long now = 0;
    private long tickDuration;
    private ProtocolServerFactory factory;
    private final MultipleFailureLatencyStrategy strategy;
    private MessageTimeoutStrategy timeoutStrategy;
    protected final org.slf4j.Logger logger;

    public NetworkMock( long tickDuration, ProtocolServerFactory factory, MultipleFailureLatencyStrategy strategy, MessageTimeoutStrategy timeoutStrategy )
    {
        this.tickDuration = tickDuration;
        this.factory = factory;
        this.strategy = strategy;
        this.timeoutStrategy = timeoutStrategy;
        logger = LoggerFactory.getLogger( NetworkMock.class );
    }

    public TestProtocolServer addServer( URI serverId )
    {
        TestProtocolServer server = newTestProtocolServer(serverId);

        debug( serverId.toString(), "joins network" );

        participants.put( serverId.toString(), server );

        return server;
    }

    protected TestProtocolServer newTestProtocolServer(URI serverId)
    {
        TestProtocolServer protocolServer = new TestProtocolServer( timeoutStrategy, factory, serverId, new InMemoryAcceptorInstanceStore(), new ServerIdElectionCredentialsProvider() );
        protocolServer.addStateTransitionListener( new StateTransitionLogger( serverId.toString() ) );
        return protocolServer;
    }

    private void debug( String participant, String string )
    {
        logger.info( "=== " + participant + " " + string );
    }

    public void removeServer( String serverId )
    {
        debug( serverId, "leaves network" );
        participants.remove( serverId );
    }

    public int tick()
    {
        // Deliver messages whose delivery time has passed
        now += tickDuration;

 //       logger.debug( "tick:"+now );

        Iterator<MessageDelivery> iter = messageDeliveries.iterator();
        while (iter.hasNext())
        {
            MessageDelivery messageDelivery = iter.next();
            if (messageDelivery.getMessageDeliveryTime() <= now)
            {
                long delay = strategy.messageDelay(messageDelivery.getMessage(), messageDelivery.getServer().toString());
                if (delay != NetworkLatencyStrategy.LOST)
                {
                    messageDelivery.getServer().process(messageDelivery.getMessage());
                }
                iter.remove();
            }
        }

        // Check and trigger timeouts
        for( TestProtocolServer testServer : participants.values() )
        {
            testServer.tick(now);
        }

        // Get all sent messages from all test servers
        List<Message> messages = new ArrayList<Message>(  );
        for( TestProtocolServer testServer : participants.values() )
        {
            testServer.sendMessages( messages );
        }
        
        // Now send them and figure out latency
        for( Message message : messages )
        {
            String to = message.getHeader( Message.TO );
            if ( to.equals( Message.BROADCAST ))
            {
                for( Map.Entry<String, TestProtocolServer> testServer : participants.entrySet() )
                {
                    if (!testServer.getKey().equals( message.getHeader( Message.FROM ) ))
                    {
                        long delay = strategy.messageDelay(message, testServer.getKey());
                        if (delay == NetworkLatencyStrategy.LOST)
                        {
                            logger.info( "Broadcasted message to " + testServer.getKey() + " was lost" );

                        } else
                        {
                            logger.info( "Broadcast to " + testServer.getKey() + ": " + message );
                            messageDeliveries.add(new MessageDelivery(now+delay, message, testServer.getValue()));
                        }
                    }
                }
            } else
            {
                long delay = 0;
                if (message.getHeader( Message.TO ).equals( message.getHeader( Message.FROM ) ))
                {
                    logger.info( "Sending message to itself; zero latency" );
                } else
                {
                    delay = strategy.messageDelay(message, to);
                }

                if (delay == NetworkLatencyStrategy.LOST)
                {
                    logger.info( "Send message to "+to+" was lost");
                } else
                {
                    TestProtocolServer server = participants.get( to );
                    logger.info("Send to " + to + ": " + message);
                    messageDeliveries.add(new MessageDelivery(now+delay, message, server));
                }
            }
        }

        return messageDeliveries.size();
    }
    
    public void tick(int iterations)
    {
        for( int i = 0; i < iterations; i++ )
        {
            tick();
        }
    }

    public void tickUntilDone()
    {
        while (tick()+totalCurrentTimeouts()>0){}
    }

    private int totalCurrentTimeouts()
    {
        int count = 0;
        for (TestProtocolServer testProtocolServer : participants.values())
        {
            count += testProtocolServer.getTimeouts().getTimeouts().size();
        }
        return count;
    }

    @Override
    public String toString()
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter out = new PrintWriter( stringWriter, true);
        out.printf( "Now:%s \n", now );
        out.printf( "Pending messages:%s \n", messageDeliveries.size() );
        out.printf( "Pending timeouts:%s \n", totalCurrentTimeouts() );

        for( TestProtocolServer testProtocolServer : participants.values() )
        {
            out.println( "  "+testProtocolServer);
        }
        return stringWriter.toString();
    }

    public Long getTime()
    {
        return now;
    }

    public List<TestProtocolServer> getServers()
    {
        return new ArrayList<TestProtocolServer>( participants.values() );
    }

    public MultipleFailureLatencyStrategy getNetworkLatencyStrategy()
    {
        return strategy;
    }

    public MessageTimeoutStrategy getTimeoutStrategy()
    {
        return timeoutStrategy;
    }

    private static class MessageDelivery
    {
        long messageDeliveryTime;
        Message<? extends MessageType> message;
        TestProtocolServer server;

        private MessageDelivery(long messageDeliveryTime, Message<? extends MessageType> message, TestProtocolServer server)
        {
            this.messageDeliveryTime = messageDeliveryTime;
            this.message = message;
            this.server = server;
        }

        public long getMessageDeliveryTime()
        {
            return messageDeliveryTime;
        }

        public Message<? extends MessageType> getMessage()
        {
            return message;
        }

        public TestProtocolServer getServer()
        {
            return server;
        }

        @Override
        public String toString()
        {
            return "Deliver "+message.getMessageType().name()+" to "+server.getServer().getServerId()+" at "+messageDeliveryTime;
        }
    }
}
