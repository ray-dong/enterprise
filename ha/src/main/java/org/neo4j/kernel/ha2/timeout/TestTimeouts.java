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
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;

/**
 * TODO
 */
public class TestTimeouts
    implements Timeouts
{
    private MessageProcessor receiver;

    private Map<Object,Message> timeouts = new HashMap<Object, Message>(  );

    public TestTimeouts( MessageProcessor receiver )
    {
        this.receiver = receiver;
    }

    @Override
    public void setTimeout( Object key, Message timeoutMessage )
    {
        timeouts.put( key, timeoutMessage );
    }

    @Override
    public void cancelTimeout( Object key )
    {
        timeouts.remove( key );
    }

    public void checkTimeouts()
    {
        for( Message message : timeouts.values() )
        {
            receiver.process( message );
        }

        timeouts.clear();
    }
}
