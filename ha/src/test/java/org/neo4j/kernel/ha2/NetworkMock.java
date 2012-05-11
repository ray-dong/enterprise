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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageType;
import org.neo4j.kernel.ha2.timeout.TimeoutStrategy;

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
    private final NetworkLatencyStrategy strategy;
    private TimeoutStrategy timeoutStrategy;

    public NetworkMock( long tickDuration, ProtocolServerFactory factory, NetworkLatencyStrategy strategy, TimeoutStrategy timeoutStrategy )
    {
        this.tickDuration = tickDuration;
        this.factory = factory;
        this.strategy = strategy;
        this.timeoutStrategy = timeoutStrategy;
    }

    public TestProtocolServer addServer( String serverId )
    {
        TestProtocolServer server = newTestProtocolServer(serverId);

        debug( serverId, "joins cluster" );

        participants.put( serverId, server );

        return server;
    }

    protected TestProtocolServer newTestProtocolServer(String serverId)
    {
        return new TestProtocolServer( timeoutStrategy, factory, serverId );
    }

    private void debug( String participant, String string )
    {
        Logger.getLogger("").info( "=== " + participant + " " + string );
    }

    public void removeServer( String serverId )
    {
        debug( serverId, "leaves cluster" );
        TestProtocolServer server = participants.get(serverId);
        server.stop();

        participants.remove( serverId );
    }

    public int tick()
    {
        // Get all messages from all test servers
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
                            Logger.getLogger("").info( "Broadcasted message to "+testServer.getKey()+" was lost");

                        } else
                        {
                            Logger.getLogger("").info("Broadcast to " + testServer.getKey() + ": " + message);
                            messageDeliveries.add(new MessageDelivery(now+delay, message, testServer.getValue()));
                        }
                    }
                }
            } else
            {
                long delay = strategy.messageDelay(message, to);
                if (delay == NetworkLatencyStrategy.LOST)
                {
                    Logger.getLogger("").info( "Send message to "+to+" was lost");
                } else
                {
                    TestProtocolServer server = participants.get( to );
                    Logger.getLogger("").info("Send to " + to + ": " + message);
                    messageDeliveries.add(new MessageDelivery(now+delay, message, server));
                }
            }
        }

        // Deliver messages whose delivery time has passed
        now += tickDuration;

        Iterator<MessageDelivery> iter = messageDeliveries.iterator();
        while (iter.hasNext())
        {
            MessageDelivery messageDelivery = iter.next();
            if (messageDelivery.getMessageDeliveryTime() <= now)
            {
                messageDelivery.getServer().process(messageDelivery.getMessage());
                iter.remove();
            }
        }

        return messageDeliveries.size();
    }
    
    public void tickUntilDone()
    {
        do
        {
            while (tick()+totalCurrentTimeouts()>0){}
            
            for( TestProtocolServer testServer : participants.values() )
            {
                testServer.tick(tickDuration);
            }
        } while (tick() > 0);
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
            return "Deliver "+message.getMessageType().name()+" to "+server+" at "+messageDeliveryTime;
        }
    }
}
