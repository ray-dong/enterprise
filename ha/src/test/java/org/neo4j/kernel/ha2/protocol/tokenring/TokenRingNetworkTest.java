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

package org.neo4j.kernel.ha2.protocol.tokenring;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.swing.SwingUtilities;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.Specifications;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ConfigProxy;
import org.neo4j.kernel.LifeSupport;
import org.neo4j.kernel.ha2.Server;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.StateTransition;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class TokenRingNetworkTest
{

    protected Logger logger;

    @Before
    public void setupLogging()
    {
        for( Handler handler : Logger.getLogger( "" ).getHandlers() )
        {
            Logger.getLogger( "" ).removeHandler( handler );
        }

        Logger.getLogger( "" ).addHandler( new Handler()
        {
            @Override
            public void publish( LogRecord record )
            {
                StringLogger.SYSTEM.logMessage( record.getMessage(), true );
            }

            @Override
            public void flush()
            {
            }

            @Override
            public void close()
                throws SecurityException
            {
            }
        } );
        logger = Logger.getLogger( "" );
    }

    @Test
    public void testSendReceive()
        throws ExecutionException, InterruptedException
    {
        Map<String, String> config = MapUtil.stringMap( "port", "1234-1244" );

        Server.Configuration configuration = ConfigProxy.config( config, Server.Configuration.class );
        LifeSupport life = new LifeSupport();
        final Server server1 = new Server( configuration );
        life.add( server1 );
        Server server2 = new Server( configuration );
        life.add( server2 );
        Server server3 = new Server( configuration );
        life.add( server3 );

        showParticipants( server1 );
        showParticipants( server2 );
        showParticipants( server3 );

        life.start();

        try
        {
            Thread.sleep( 10000 );
        }
        catch( InterruptedException e )
        {
            e.printStackTrace();
        }

        logger.info( "Shutting down" );
        life.shutdown();
    }

    private void showParticipants( final Server server )
    {
        server.addStateTransitionListener( new StateTransitionListener()
        {
            @Override
            public void stateTransition( StateTransition transition )
            {
                if (transition.getNewState().equals( transition.getOldState() ))
                    return;

                if( Specifications.<Object>in( TokenRingState.slave, TokenRingState.master )
                    .satisfiedBy( transition.getNewState() ) )
                {
                    SwingUtilities.invokeLater( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            Future<Iterable<RingParticipant>> participants1 = server.newClient( TokenRing.class )
                                .getParticipants();

                            logger.info( "Found ring participants from server:" );
                            try
                            {
                                for( RingParticipant ringParticipant : participants1.get() )
                                {
                                    logger.info( ringParticipant.toString() );
                                }
                            }
                            catch( InterruptedException e )
                            {
                                e.printStackTrace();
                            }
                            catch( ExecutionException e )
                            {
                                e.printStackTrace();
                            }
                        }
                    });
                }
            }
        } );
    }
}
