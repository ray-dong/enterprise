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

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ConfigProxy;
import org.neo4j.kernel.LifeSupport;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.LifecycleAdapter;
import org.neo4j.kernel.impl.util.StringLogger;

import java.util.Map;

/**
 * TODO
 */
public class NetworkSendReceiveTest
{
    @Test
    public void testSendReceive()
    {
        LifeSupport life = new LifeSupport();
        life.add(new Server());
        life.add(new Server());
        life.start();

        try
        {
            Thread.sleep(5000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        life.shutdown();
    }

    private class Server
        implements Lifecycle
    {

        private final LifeSupport life = new LifeSupport();

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
            Map<String, String> config = MapUtil.stringMap("port", "1234");

            NetworkChannels channels = new NetworkChannels(StringLogger.SYSTEM);

            final NetworkMessageReceiver receiver = new NetworkMessageReceiver(ConfigProxy.config(config, NetworkMessageReceiver.Configuration.class), StringLogger.SYSTEM, channels);
            final NetworkMessageSender sender = new NetworkMessageSender( StringLogger.SYSTEM, channels );

            life.add(receiver);
            life.add(sender);
            life.add(new LifecycleAdapter()
            {
                @Override
                public void start() throws Throwable
                {
                    receiver.addMessageListener(new NetworkMessageListener()
                    {
                        @Override
                        public void received(Object message)
                        {
                            System.out.println(message);
                        }
                    });
                }
            });
            life.add(new LifecycleAdapter()
            {
                @Override
                public void start() throws Throwable
                {
                    sender.send("neo4j://127.0.0.1:1234", "Hello World");
                }
            });

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
    }
}
