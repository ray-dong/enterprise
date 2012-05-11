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
package org.neo4j.kernel.haonefive;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CountDownLatch;

public class StateSwitchBlock
{
    private volatile CountDownLatch latch;
    
    public synchronized void enter()
    {
        if ( latch != null )
            return;
        latch = new CountDownLatch( 1 );
    }

    public boolean await( int seconds )
    {
        CountDownLatch localLatch = latch;
        if ( localLatch == null )
            return true;
        
        try
        {
            return localLatch.await( seconds, SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( "Timed out waiting for database to switch state", e );
        }
    }

    public synchronized void exit()
    {
        if ( latch == null )
            throw new IllegalStateException();
        latch.countDown();
        latch = null;
    }
}
