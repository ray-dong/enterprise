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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageType;

/**
 * Timeout management for state machines
 */
public class Timeouts
{
    private long now = 0;

    private MessageProcessor receiver;
    private TimeoutStrategy timeoutStrategy;

    private Map<Object,Timeout> timeouts = new HashMap<Object, Timeout>(  );
    private List<Map.Entry<Object,Timeout>> triggeredTimeouts = new ArrayList<Map.Entry<Object,Timeout>>(  );

    public Timeouts( MessageProcessor receiver, TimeoutStrategy timeoutStrategy )
    {
        this.receiver = receiver;
        this.timeoutStrategy = timeoutStrategy;
    }

    public void setTimeout( Object key, Message<? extends MessageType> timeoutMessage )
    {
        timeouts.put( key, new Timeout( now + timeoutStrategy.timeoutFor( timeoutMessage ), timeoutMessage ));
    }

    public void cancelTimeout( Object key )
    {
        Timeout timeout = timeouts.remove( key );
        if (timeout != null)
            timeoutStrategy.timeoutCancelled(timeout.timeoutMessage);
    }

    public Map<Object, Timeout> getTimeouts()
    {
        return timeouts;
    }

    public Message<? extends MessageType> getTimeoutMessage( String timeoutName )
    {
        Timeout timeout = timeouts.get( timeoutName );
        if (timeout != null)
            return timeout.getTimeoutMessage();
        else
            return null;
    }

    public void tick(long time)
    {
        synchronized(this)
        {
            // Time has passed
            now = time;

            timeoutStrategy.tick( now );

            // Check if any timeouts needs to be triggered
            triggeredTimeouts.clear();
            for( Map.Entry<Object,Timeout> timeout : timeouts.entrySet() )
            {
                if (timeout.getValue().checkTimeout(now))
                    triggeredTimeouts.add( timeout );
            }

            // Remove all timeouts that were triggered
            for( Map.Entry<Object,Timeout> triggeredTimeout : triggeredTimeouts )
            {
                timeouts.remove( triggeredTimeout.getKey() );
            }
        }

        // Trigger timeouts
        for( Map.Entry<Object, Timeout> triggeredTimeout : triggeredTimeouts )
        {
            triggeredTimeout.getValue().trigger( receiver );
        }
    }

    public class Timeout
    {
        private long timeout;
        private Message<? extends MessageType> timeoutMessage;

        public Timeout( long timeout, Message<? extends MessageType> timeoutMessage )
        {
            this.timeout = timeout;
            this.timeoutMessage = timeoutMessage;
        }

        public Message<? extends MessageType> getTimeoutMessage()
        {
            return timeoutMessage;
        }

        public boolean checkTimeout(long now)
        {
            if (now>=timeout)
            {
                timeoutStrategy.timeoutTriggered(timeoutMessage);

                return true;
            } else
            {
                return false;
            }
        }

        public void trigger( MessageProcessor receiver )
        {
            receiver.process( timeoutMessage );
        }

        @Override
        public String toString()
        {
            return timeout+": "+timeoutMessage;
        }
    }
}
