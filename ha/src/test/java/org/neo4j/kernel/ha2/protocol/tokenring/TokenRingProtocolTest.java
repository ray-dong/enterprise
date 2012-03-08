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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.com2.message.MessageType;
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.StateTransitionExpectations;
import org.neo4j.kernel.ha2.StateTransitionExpectations.ExpectationsBuilder;
import org.neo4j.kernel.ha2.TestServer;
import org.neo4j.kernel.ha2.Verifier;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.State;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.*;

/**
 * TODO
 */
public class TokenRingProtocolTest
{
    private static final String ID1 = "1";
    private static final String ID2 = "2";
    private static final String ID3 = "3";
    
    private NetworkMock network;
    private StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations;
    protected Logger logger;

    @Before
    public void setupLogging()
    {
        logger = Logger.getLogger( "" );

        for( Handler handler : logger.getHandlers() )
        {
            logger.removeHandler( handler );
        }

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter( new Formatter()
        {
            @Override
            public String format( LogRecord record )
            {
                return record.getMessage()+"\n";
            }
        });

        logger.addHandler( handler );
    }
    
    @Before
    public void setupEnvironment()
    {
        network = new NetworkMock();
        expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();
    }
    
    @After
    public void verifyWhatHappened()
    {
        expectations.verify();
    }
    
    /* TESTS TO WRITE
     * whenEveryoneStartsAtTheSameTimeTheRingIsFormedByConsensus
     * formSizeTwoRingWithAnotherMember
     * leaveRingOfSizeTwoResultingInTwoStrayParticipants
     * leaveRingOfSizeThreeResultingInRingOfSizeTwo
     * leaveRingOfSizeFourResultingInRingOfSizeThree
     * leaveAndThenRejoinSameRingInTheSamePlace
     * leaveAndThenRejoinSameRingInAnotherPlace
     * joinFailsMidWay (also permutations for different points in the conversation)
     * leaveFailsMidWay (also permutations for different points in the conversation)
     * twoMembersJoinEstablishedRingAtTheSameTimeByAskingDifferentMembers
     * twoMembersJoinEstablishedRingAtTheSameTimeByAskingDifferentDirectlyConnectdMembers
     * twoMembersJoinEstablishedRingAtTheSameTimeByAskingSameRingMember
     * twoNotDirectlyConnectedMembersLeaveAtTheSameTime
     * twoDirectlyConnectedMembersLeaveAtTheSameTime
     * oneJoinsEstablishedRingWhileTheOneBeforeLeavesAtTheSameTime
     * oneJoinsEstablishedRingWhileTheOneAfterLeavesAtTheSameTime
     * 
     * Introduce failures at any point for all the tests above? exploooosion of tests
     * if we don't build a super fancy framework for that.
     * 
     * FOR LATER
     * sendTokenAroundTheRing
     * leaveRingWhenNotHavingToken
     * joinRingWhileRingIsPassingAroundToken
     */ 
    
    @Test
    public void formSizeTwoRingWithAnotherMember() throws Exception
    {
        TokenRing member1 = newMember( ID1,
                TokenRingMessage.start, initial,
                TokenRingMessage.discoveryTimedOut, master );
        network.tickUntilDone();
        assertRingParticipants( member1.getParticipants(), ID1 );
        verifyNeighbours( ID1, ID1, ID1 );
        
        TokenRing member2 = newMember( ID2,
                TokenRingMessage.start, initial,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        assertRingParticipants( member2.getParticipants(), ID1, ID2 );
        verifyNeighbours( ID2, ID1, ID2 );
        assertRingParticipants( member1.getParticipants(), ID1, ID2 );
        verifyNeighbours( ID1, ID2, ID1 );
    }
    
    @Test
    public void leaveRingOfSizeTwoResultingInTwoStrayParticipants() throws Exception
    {
        newMember( ID1,
                TokenRingMessage.start, initial,
                TokenRingMessage.discoveryTimedOut, master );
        network.tickUntilDone();
        newMember( ID2,
                TokenRingMessage.start, initial,
                TokenRingMessage.ringDiscovered, slave,
                TokenRingMessage.leaveRing, start );
        network.tickUntilDone();
        verifyNeighbours( ID2, ID1, ID2 );
        verifyNeighbours( ID1, ID2, ID1 );
        
        network.removeServer( ID2 );
        verifyNeighbours( ID1, ID1, ID1 );
    }
    
    @Test
    public void leaveRingOfSizeThreeResultingInRingOfSizeTwo() throws Exception
    {
        newMember( ID1,
                TokenRingMessage.start, initial,
                TokenRingMessage.discoveryTimedOut, master );
        network.tickUntilDone();
        newMember( ID2,
                TokenRingMessage.start, initial,
                TokenRingMessage.ringDiscovered, slave,
                TokenRingMessage.leaveRing, start );
        network.tickUntilDone();
        verifyNeighbours( ID2, ID1, ID2 );
        verifyNeighbours( ID1, ID2, ID1 );
        newMember( ID3,
                TokenRingMessage.start, initial,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        verifyNeighboursWithUnknownPosition( ID2, ID1, ID3 );
        verifyNeighboursWithUnknownPosition( ID1, ID2, ID3 );
        verifyNeighboursWithUnknownPosition( ID1, ID3, ID2 );
        
        network.removeServer( ID2 );
        verifyNeighbours( ID3, ID1, ID3 );
        verifyNeighbours( ID1, ID3, ID1 );
    }
    
    @Test
    public void startNewRingWith3ParticipantsAndShutItDown()
    {
        String server1 = "server1";
        network.addServer( server1).addStateTransitionListener( expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.discoveryTimedOut, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.leaveRing, start )
            .build( server1 ) ).newClient( TokenRing.class ).start();

        network.tickUntilDone();

        String server2 = "server2";
        network.addServer( server2 ). addStateTransitionListener( expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.discoverRing, slave )
            .expect( TokenRingMessage.newAfter, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .expect( TokenRingMessage.leaveRing, start )
            .build( server2 ) ).newClient( TokenRing.class ).start();

        network.tickUntilDone();

        String server3 = "server3";
        network.addServer( server3 ).addStateTransitionListener( expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.newAfter, slave )
            .expect( TokenRingMessage.newAfter, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .expect( TokenRingMessage.leaveRing, start )
            .build( server3 ) ).newClient( TokenRing.class ).start();

        network.tickUntilDone();

//        expectations.printRemaining(logger);

        network.removeServer( server1 );
        network.removeServer( server2 );
        network.removeServer( server3 );

        network.tickUntilDone();
    }

    @Test
    public void startNewRingWith3ParticipantsAndSlavesLeave()
    {
        String server1 = "server1";
        network.addServer( server1 ).addStateTransitionListener(expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.discoveryTimedOut, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.discoverRing, master )
            .expect( TokenRingMessage.newAfter, master )
            .expect( TokenRingMessage.newAfter, master )
            .expect( TokenRingMessage.newBefore, master )
            .build( server1 ) ).newClient( TokenRing.class ).start();

        network.tickUntilDone();

        String server2 = "server2";
        network.addServer( server2 ).addStateTransitionListener( expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.discoverRing, slave )
            .expect( TokenRingMessage.newAfter, slave )
            .expect( TokenRingMessage.leaveRing, start )
            .build( server2 ) ).newClient( TokenRing.class ).start();

        network.tickUntilDone();

        String server3 = "server3";
        network.addServer( server3 ).addStateTransitionListener( expectations.newExpectations().includeUnchangedStates()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.newBefore, slave )
            .expect( TokenRingMessage.leaveRing, start )
            .build( server3 ) ).newClient( TokenRing.class ).start();

        network.tickUntilDone();

        network.removeServer( server2 );
        network.removeServer( server3 );
        
        network.tickUntilDone();
    }

    @Test
    public void startNewRingWith3ParticipantsAndSendTokenAround()
    {
        String participant1 = "server1";
        TestServer server1 = network.addServer( participant1 ).addStateTransitionListener( expectations.newExpectations()
                                                                                               .expect( TokenRingMessage.start, initial )
                                                                                               .expect( TokenRingMessage.discoveryTimedOut, master )
                                                                                               .expect( TokenRingMessage.sendToken, slave )
                                                                                               .build( participant1 ) );
        server1.newClient( TokenRing.class ).start();
        
        network.tickUntilDone();

        String participant2 = "server2";
        TestServer server2 = network.addServer( participant2 ).addStateTransitionListener( expectations.newExpectations()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .expect( TokenRingMessage.sendToken, slave )
            .build( participant2 ) );
        server2.newClient( TokenRing.class ).start();
        
        network.tickUntilDone();

        String participant3 = "server3";
        TestServer server3 = network.addServer( participant3 ).addStateTransitionListener( expectations.newExpectations()
            .expect( TokenRingMessage.start, initial )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .build( participant3 ) );
        server3.newClient( TokenRing.class ).start();
        
        network.tickUntilDone();

        TokenRing tokenRing1 = server1.newClient( TokenRing.class );
        tokenRing1.sendToken();

        network.tickUntilDone();

        TokenRing tokenRing2 = server2.newClient( TokenRing.class );
        tokenRing2.sendToken();

        network.tickUntilDone();
    }

    private void verifyNeighbours( final String before, final String me, final String after )
    {
        network.verifyState( me, new Verifier<TokenRingContext>()
        {
            @Override
            public void verify( TokenRingContext state )
            {
                assertEquals( before, state.getNeighbours().getBefore().getServerId() );
                assertEquals( me, state.getMe().getServerId() );
                assertEquals( after, state.getNeighbours().getAfter().getServerId() );
            }
        } );
    }
    
    private void verifyNeighboursWithUnknownPosition( final String one, final String me, final String other )
    {
        network.verifyState( me, new Verifier<TokenRingContext>()
        {
            @Override
            public void verify( TokenRingContext state )
            {
                String before = state.getNeighbours().getBefore().getServerId();
                String after = state.getNeighbours().getAfter().getServerId();
                assertFalse( "Before and after both are '" + before + "', but was expected to be '" + one + "','" + other + "'", before.equals( after ) );
                assertTrue( "Expected one of '" + me + "'s neighbors to be '" + one + "', but was '" + before + "','" + after + "'", one.equals( before ) || one.equals( after ) );
                assertEquals( "Expected me to be '" + me + "', but was '" + state.getMe().getServerId() + "'", me, state.getMe().getServerId() );
                assertTrue( "Expected other of '" + me + "'s neighbors to be '" + one + "', but was '" + before + "','" + after + "'", other.equals( before ) || other.equals( after ) );
            }
        } );
    }
    
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    private TokenRing newMember( String id, Enum<?>... alternatingExpectedMessageAndState )
    {
        TestServer server = network.addServer( id );
        if ( alternatingExpectedMessageAndState.length > 0 )
        {
            ExpectationsBuilder expectationBuilder = expectations.newExpectations();
            for ( int i = 0; i < alternatingExpectedMessageAndState.length; i++ )
                expectationBuilder.expect( (MessageType) alternatingExpectedMessageAndState[i++], (State) alternatingExpectedMessageAndState[i] );
            server.addStateTransitionListener( expectationBuilder.build( id ) );
        }
        TokenRing member = server.newClient( TokenRing.class );
        member.start();
        return member;
    }
    
    private void assertRingParticipants( Future<Iterable<RingParticipant>> participants,
            String... expectedParticipantIds ) throws InterruptedException, ExecutionException
    {
        network.tickUntilDone();
        Set<String> expectedSet = new HashSet<String>( Arrays.asList( expectedParticipantIds ) );
        for ( RingParticipant participant : participants.get() )
            assertTrue( expectedSet.remove( participant.getServerId() ) );
        assertTrue( "Didn't get expected items: " + expectedSet, expectedSet.isEmpty() );
    }
}
