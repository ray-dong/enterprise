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

package org.neo4j.com_2;

import org.junit.Test;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageType;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * TODO
 */
public class NetworkSendReceiveTest
{
    public enum TestMessage
        implements MessageType
    {
        helloWorld;
    }

    @Test
    public void testSendReceive()
    {
        LifeSupport life = new LifeSupport();

        Server server1;
        {
            Server.Configuration config = new Server.Configuration()
            {
                
                @Override
                public int[] port( int[] defaultPortRange, int min, int max )
                {
                    return new int[] { 1234, 1234 };
                }
                
                @Override
                public String address( String def )
                {
                    return def;
                }
            };
            
            life.add(server1 = new Server( config ));
        }
        {
            Server.Configuration config = new Server.Configuration()
            {
                
                @Override
                public int[] port( int[] defaultPortRange, int min, int max )
                {
                    return new int[] { 1235, 1235 };
                }
                
                @Override
                public String address( String def )
                {
                    return def;
                }
            };
            life.add(new Server( config ));
        }
        
        life.start();
        
        server1.process( Message.to( TestMessage.helloWorld, "neo4j://127.0.0.1:1235", "Hello World" ) );
        
        try
        {
            Thread.sleep(2000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        life.shutdown();
    }

    private static class Server
        implements Lifecycle, MessageProcessor
    {

        protected NetworkNode node;

        public interface Configuration
            extends NetworkNode.Configuration
        {}
        
        private final LifeSupport life = new LifeSupport();
        
        private Server( Configuration config )
        {
            node = new NetworkNode(config, StringLogger.SYSTEM);

            life.add( node );
            life.add(new LifecycleAdapter()
            {
                @Override
                public void start() throws Throwable
                {
                    node.addMessageProcessor( new MessageProcessor()
                    {
                        @Override
                        public void process( Message<? extends MessageType> message )
                        {
                            System.out.println( message );
                        }
                    } );
                }
            });
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {

            life.start();
        }

        @Override
        public void stop() throws Throwable
        {
            life.stop();
        }

        @Override
        public void shutdown() throws Throwable
        {
        }

        @Override
        public void process( Message<? extends MessageType> message )
        {
            node.process( message );
        }
    }
}
