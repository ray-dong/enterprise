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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.kernel.Lifecycle;

/**
 * TODO
 */
public class TimeoutMessageFailureHandler
    extends AbstractMessageFailureHandler
    implements Lifecycle
{
    private ScheduledExecutorService expectationScheduler;
    
    public TimeoutMessageFailureHandler( MessageProcessor incoming, MessageSource outgoing, MessageSource source )
    {
        super( incoming, outgoing, source );
    }

    @Override
    public void init()
        throws Throwable
    {
        expectationScheduler = Executors.newScheduledThreadPool( 3 );
    }

    @Override
    public void start()
        throws Throwable
    {
    }

    @Override
    public void stop()
        throws Throwable
    {
    }

    @Override
    public void shutdown()
        throws Throwable
    {
        expectationScheduler.shutdown();
    }

    @Override
    protected void expectation( AbstractMessageFailureHandler.ExpectationFailure expectationFailure )
    {
        super.expectation( expectationFailure );

        expectationScheduler.schedule( expectationFailure, 3, TimeUnit.SECONDS );
    }
}
