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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.LifeSupport;
import org.neo4j.kernel.ha2.Goal;
import org.neo4j.kernel.ha2.Goal.GoalNotMetException;
import org.neo4j.kernel.ha2.Goal.SubGoal;
import org.neo4j.kernel.ha2.NetworkedServerFactory;
import org.neo4j.kernel.ha2.Server;
import org.neo4j.kernel.ha2.StateTransitionExpectations;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.*;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.*;

/**
 * TODO share expected conditions or even more with {@link TokenRingProtocolTest}
 */
public class TokenRingNetworkTest
{
    private static final String ID3 = "3";
    private static final String ID2 = "2";
    private static final String ID1 = "1";
    private static final Map<String, String> config = MapUtil.stringMap( "port", "1234-1244" );
    protected Logger logger;
    private LifeSupport life;
    private NetworkedServerFactory serverFactory;
    private Map<String,Pair<Server,TokenRing>> members = new HashMap<String, Pair<Server,TokenRing>>();
    private StateTransitionExpectations<TokenRingContext, TokenRingMessage, TokenRingState> expectations =
            new StateTransitionExpectations<TokenRingContext, TokenRingMessage, TokenRingState>();
    private Goal goal = new Goal( 10000, 1000 );
    
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
    
    @Before
    public void setupNetwork()
    {
        life = new LifeSupport();
        serverFactory = new NetworkedServerFactory( life );
        goal.add( expectationsAreFulfilledGoal() );
    }
    
    @After
    public void closeNetwork() throws GoalNotMetException
    {
        goal.await();
        life.shutdown();
        logger.info( "Shutting down" );
        expectations.verify();
    }
    
    private Server newServer( String id, Enum<?>... alternatingExpectedMessageAndState )
    {
        Server server = serverFactory.newNetworkedServer( config );
        if ( alternatingExpectedMessageAndState.length > 0 )
            server.addStateTransitionListener( expectations.newExpectations( alternatingExpectedMessageAndState ).build( id ) );
        members.put( id, Pair.of( server, (TokenRing)null ) );
        return server;
    }

    private TokenRing getMember( String id )
    {
        Pair<Server, TokenRing> member = members.get( id );
        if ( member == null )
            throw new IllegalArgumentException( "Member '" + id + "' not found" );
        return member.other();
    }
    
    private void start()
    {
        life.start();
        Map<String,Pair<Server,TokenRing>> startedMembers = new HashMap<String, Pair<Server,TokenRing>>();
        for ( Map.Entry<String,Pair<Server,TokenRing>> member : members.entrySet() )
            startedMembers.put( member.getKey(), Pair.of( member.getValue().first(), member.getValue().first().newClient( TokenRing.class ) ) );
        members = startedMembers;
    }

    @Test
    public void whenEveryoneStartsAtTheSameTimeTheRingIsFormedByConsensus() throws Exception
    {
        Server server1 = newServer( ID1,
                TokenRingMessage.joinRing, joiningRing,
                // Becomes master since the one with the lowest server id will take on the role as master in
                // the scenario where there are many confused participants trying to form a ring.
                TokenRingMessage.ringDiscoveryTimedOut, master );
        Server server2 = newServer( ID2,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        Server server3 = newServer( ID3,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        start();
        awaitExpectations();
        
        assertRingParticipants( getMember( ID1 ), server1, server2, server3 );
        assertRingParticipants( getMember( ID2 ), server1, server2, server3 );
        assertRingParticipants( getMember( ID3 ), server1, server2, server3 );
    }

    private void awaitExpectations() throws GoalNotMetException
    {
        goal.await();
    }

    private SubGoal expectationsAreFulfilledGoal()
    {
        return new Goal.SubGoal()
        {
            @Override
            public boolean met()
            {
                return expectations.areFulfilled();
            }
        };
    }
    
    private void assertRingParticipants( TokenRing member, Server... expectedParticipants ) throws InterruptedException, ExecutionException
    {
        Future<Iterable<RingParticipant>> participants = member.getParticipants();
        Set<String> expectedSet = new HashSet<String>();
        for ( Server expected : expectedParticipants )
            expectedSet.add( expected.getServerId() );
        for ( RingParticipant participant : participants.get() )
            assertTrue( "Unexpected ring participant '" + participant.getServerId() + "'", expectedSet.remove( participant.getServerId() ) );
        assertTrue( "Didn't get expected items: " + expectedSet, expectedSet.isEmpty() );
    }

//    @Test
//    public void testSendReceive()
//            throws ExecutionException, InterruptedException
//    {
//        onJoinShowParticipants( newServer( ID1 ) );
//        onJoinShowParticipants( newServer( ID2 ) );
//        onJoinShowParticipants( newServer( ID3 ) );
//        start();
//
//        // TODO don't sleep, but instead wait for some goal
//        Thread.sleep( 10000 );
//    }
    
//    private void onJoinShowParticipants( final Server server )
//    {
//        server.addStateTransitionListener( new StateTransitionListener()
//        {
//            @Override
//            public void stateTransition( StateTransition transition )
//            {
//                if (transition.getNewState().equals( transition.getOldState() ))
//                    return;
//
//                if( Specifications.<Object>in( TokenRingState.slave, TokenRingState.master )
//                    .satisfiedBy( transition.getNewState() ) )
//                {
//                    SwingUtilities.invokeLater( new Runnable()
//                    {
//                        @Override
//                        public void run()
//                        {
//                            Future<Iterable<RingParticipant>> participants1 = server.newClient( TokenRing.class )
//                                .getParticipants();
//
//                            logger.info( "Found ring participants from server:" );
//                            try
//                            {
//                                for( RingParticipant ringParticipant : participants1.get() )
//                                {
//                                    logger.info( ringParticipant.toString() );
//                                }
//                            }
//                            catch( InterruptedException e )
//                            {
//                                e.printStackTrace();
//                            }
//                            catch( ExecutionException e )
//                            {
//                                e.printStackTrace();
//                            }
//                        }
//                    });
//                }
//            }
//        } );
//    }
}
