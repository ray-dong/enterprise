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

package org.neo4j.kernel.ha2.timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageType;
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.Lifecycle;

/**
 * TODO
 */
public class TimeoutsService
    implements Lifecycle, Timeouts
{
    private ScheduledExecutorService scheduler;
    private TimeoutStrategy timeoutStrategy;
    private MessageProcessor output;

    private Map<Object,Timeout> timeouts = new HashMap<Object, Timeout>(  );

    public TimeoutsService( TimeoutStrategy timeoutStrategy, MessageProcessor output )
    {
        this.timeoutStrategy = timeoutStrategy;
        this.output = output;
    }

    @Override
    public void init()
        throws Throwable
    {
    }

    @Override
    public void start()
        throws Throwable
    {
        scheduler = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory( "timeout" ));
    }

    @Override
    public void stop()
        throws Throwable
    {
        scheduler.shutdownNow();
    }

    @Override
    public void shutdown()
        throws Throwable
    {
    }

    public void setTimeout(Object key, Message<? extends MessageType> timeoutMessage)
    {
        Timeout timeout = new Timeout( key, timeoutMessage );
        timeouts.put( key, timeout );
        scheduler.schedule( timeout, timeoutStrategy.timeoutFor( timeoutMessage ), TimeUnit.MILLISECONDS );
    }

    public void cancelTimeout(Object key)
    {
        Timeout timeout = timeouts.get( key );
        if (timeout != null)
            timeout.cancel();
    }

    private class Timeout
        implements Runnable
    {
        private boolean cancelled = false;
        private Object key;
        private Message<? extends MessageType> message;

        private Timeout( Object key, Message<? extends MessageType> message )
        {
            this.key = key;
            this.message = message;
        }

        public void cancel()
        {
            cancelled = true;
            timeouts.remove( key );
        }

        @Override
        public void run()
        {
            if (!cancelled)
            {
                timeouts.remove( key );
                System.out.println( "TIMEOUT TRIGGERED:"+message );
                output.process( message );
            }
        }
    }
}
