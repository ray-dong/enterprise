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

import java.util.Map;
import org.junit.Test;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageType;
import org.neo4j.helpers.collection.MapUtil;
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
            life.add(server1 = new Server( MapUtil.stringMap( NetworkNode.cluster_port.name(), "1234" ) ));
        }
        {
            life.add(new Server( MapUtil.stringMap( NetworkNode.cluster_port.name(), "1235" ) ));
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

        private final LifeSupport life = new LifeSupport();
        
        private Server( Map<String,String> config )
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
