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
import org.neo4j.kernel.ha2.NetworkMock;
import org.neo4j.kernel.ha2.ScriptableNetworkFailureStrategy;
import org.neo4j.kernel.ha2.StateTransitionExpectations;
import org.neo4j.kernel.ha2.StateTransitionExpectations.ExpectationsBuilder;
import org.neo4j.kernel.ha2.TestServer;
import org.neo4j.kernel.ha2.Verifier;
import org.neo4j.kernel.ha2.protocol.RingParticipant;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.ha2.protocol.tokenring.TokenRingState.*;

/**
 * TODO share expected conditions or even more with {@link TokenRingNetworkTest}
 */
public class TokenRingProtocolTest
{
    private static final String ID1 = "1";
    private static final String ID2 = "2";
    private static final String ID3 = "3";
    private static final String ID4 = "4";
    
    private NetworkMock network;
    private StateTransitionExpectations<TokenRingContext, TokenRingMessage> expectations;
    protected Logger logger;
    protected ScriptableNetworkFailureStrategy failures;

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
        failures = new ScriptableNetworkFailureStrategy();
        network = new NetworkMock( failures );
        expectations = new StateTransitionExpectations<TokenRingContext, TokenRingMessage>();
    }
    
    @After
    public void verifyWhatHappened()
    {
        expectations.verify();
    }
    
    /* TODO TESTS TO WRITE
     * slaveLeavesAndThenRejoinSameRingInTheSamePlace
     * slaveLeavesAndThenRejoinSameRingInAnotherPlace
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
     * leaveRingWhenNotHavingToken (slave)
     * leaveRingWhenHavingToken (master)
     * joinRingWhileRingIsPassingAroundToken
     */ 
    
    @Test
    public void whenEveryoneStartsAtTheSameTimeTheRingIsFormedByConsensus() throws Exception
    {
        TokenRing member1 = newMember( ID1,
                TokenRingMessage.joinRing, joiningRing,
                // Becomes master since the one with the lowest server id will take on the role as master in
                // the scenario where there are many confused participants trying to form a ring.
                TokenRingMessage.ringDiscoveryTimedOut, master );
        TokenRing member2 = newMember( ID2,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        TokenRing member3 = newMember( ID3,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        
        assertRingParticipants( member1, ID1, ID2, ID3 ); 
        assertRingParticipants( member2, ID1, ID2, ID3 ); 
        assertRingParticipants( member3, ID1, ID2, ID3 ); 
        verifyRingSizeThreeNeighbors( ID1, ID2, ID3 );
    }
    
    @Test
    public void whenEveryoneStartsAtTheSameTimeTheRingIsFormedByConsensusVerbose() throws Exception
    {
        TokenRing member1 = newMember( ID1, true,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.discoverRing, joiningRing,
                TokenRingMessage.discoverRing, joiningRing,
                TokenRingMessage.joining, joiningRing,
                TokenRingMessage.joining, joiningRing,
                // Becomes master since the one with the lowest server id will take on the role as master in
                // the scenario where there are many confused participants trying to form a ring.
                TokenRingMessage.ringDiscoveryTimedOut, master,
                TokenRingMessage.getParticipants, master,
                TokenRingMessage.getRingParticipantsResponse, master,
                TokenRingMessage.getRingParticipants, master,
                TokenRingMessage.getRingParticipantsResponse, master );
        TokenRing member2 = newMember( ID2, true,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.discoverRing, joiningRing,
                TokenRingMessage.discoverRing, joiningRing,
                TokenRingMessage.joining, joiningRing,
                TokenRingMessage.joining, joiningRing,
                TokenRingMessage.ringDiscoveryTimedOut, joiningRing,
                TokenRingMessage.ringDiscovered, slave,
                TokenRingMessage.getRingParticipantsResponse, slave,
                TokenRingMessage.getParticipants, slave,
                TokenRingMessage.getRingParticipantsResponse, slave,
                TokenRingMessage.getRingParticipants, slave );
        TokenRing member3 = newMember( ID3, true,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.discoverRing, joiningRing,
                TokenRingMessage.discoverRing, joiningRing,
                TokenRingMessage.joining, joiningRing,
                TokenRingMessage.joining, joiningRing,
                TokenRingMessage.ringDiscoveryTimedOut, joiningRing,
                TokenRingMessage.ringDiscovered, slave,
                TokenRingMessage.getRingParticipants, slave,
                TokenRingMessage.getRingParticipantsResponse, slave,
                TokenRingMessage.getParticipants, slave,
                TokenRingMessage.getRingParticipantsResponse, slave );
        network.tickUntilDone();
        
        assertRingParticipants( member1, ID1, ID2, ID3 ); 
        assertRingParticipants( member2, ID1, ID2, ID3 ); 
        assertRingParticipants( member3, ID1, ID2, ID3 ); 
        verifyRingSizeThreeNeighbors( ID1, ID2, ID3 );
    }
    
    @Test
    public void formSizeTwoRingWithAnotherMember() throws Exception
    {
        TokenRing member1 = newMember( ID1,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscoveryTimedOut, master );
        network.tickUntilDone();
        assertRingParticipants( member1, ID1 );
        verifyNeighbours( ID1, ID1, ID1 );
        
        TokenRing member2 = newMember( ID2,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        verifyRingSizeTwoNeighbors( ID1, ID2 );
        assertRingParticipants( member1, ID1, ID2 );
        assertRingParticipants( member2, ID1, ID2 );
    }
    
    @Test
    public void slaveLeavesRingOfSizeTwoResultingInTwoStrayParticipants() throws Exception
    {
        newMember( ID1,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscoveryTimedOut, master );
        network.tickUntilDone();
        newMember( ID2,
                   TokenRingMessage.joinRing, joiningRing,
                   TokenRingMessage.ringDiscovered, slave,
                   TokenRingMessage.leaveRing, start );
        network.tickUntilDone();
        verifyRingSizeTwoNeighbors( ID1, ID2 );
        
        network.removeServer( ID2 );
        verifyNeighbours( ID1, ID1, ID1 );
    }
    
    @Test
    public void slaveLeavesRingOfSizeThreeResultingInRingOfSizeTwo() throws Exception
    {
        TokenRing member1 = newMember( ID1,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscoveryTimedOut, master );
        network.tickUntilDone();
        newMember( ID2,
                   TokenRingMessage.joinRing, joiningRing,
                   TokenRingMessage.ringDiscovered, slave,
                   TokenRingMessage.leaveRing, start );
        network.tickUntilDone();
        verifyNeighbours( ID2, ID1, ID2 );
        verifyNeighbours( ID1, ID2, ID1 );
        newMember( ID3,
                   TokenRingMessage.joinRing, joiningRing,
                   TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        assertRingParticipants( member1, ID1, ID2, ID3 );
        verifyRingSizeThreeNeighbors( ID1, ID2, ID3 );
        
        network.removeServer( ID2 );
        assertRingParticipants( member1, ID1, ID3 );
        verifyRingSizeTwoNeighbors( ID1, ID3 );
    }
    
    @Test
    public void slaveLeavesRingOfSizeFourResultingInRingOfSizeThree() throws Exception
    {
        newMember( ID1 );
        network.tickUntilDone();
        newMember( ID2 );
        network.tickUntilDone();
        TokenRing member3 = newMember( ID3 );
        network.tickUntilDone();
        TokenRing member4 = newMember( ID4,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave,
                TokenRingMessage.leaveRing, start );
        network.tickUntilDone();
        
        assertRingParticipants( member4, ID1, ID2, ID3, ID4 );
        network.removeServer( ID4 );
        assertRingParticipants( member3, ID1, ID2, ID3 );
        verifyRingSizeThreeNeighbors( ID1, ID2, ID3 );
    }
    
    @Test
    public void masterLeavesSizeThreeRingAndRejoinsAsSlave() throws Exception
    {
        TokenRing member1 = newMember( ID1,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscoveryTimedOut, master,
                TokenRingMessage.leaveRing, start,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        TokenRing member2 = newMember( ID2,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave,
                TokenRingMessage.becomeMaster, master );
        network.tickUntilDone();
        newMember( ID3,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        
        // Here 1 is master and it will hand over the master role to the one after
        assertRingParticipants( member1, ID1, ID2, ID3 );
        member1.leaveRing();
        network.tickUntilDone();
        assertRingParticipants( member2, ID2, ID3 );
        
        // Rejoin
        member1.joinRing();
        network.tickUntilDone();
        assertRingParticipants( member1, ID1, ID2, ID3 );
        assertRingParticipants( member2, ID1, ID2, ID3 );
    }
    
    @Test
    public void slaveLeavesSizeThreeRingAndRejoinsAsSlave() throws Exception
    {
        newMember( ID1,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscoveryTimedOut, master );
        network.tickUntilDone();
        TokenRing member2 = newMember( ID2,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave,
                TokenRingMessage.leaveRing, start,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        TokenRing member3 = newMember( ID3,
                TokenRingMessage.joinRing, joiningRing,
                TokenRingMessage.ringDiscovered, slave );
        network.tickUntilDone();
        
        // Here 1 is master and it will hand over the master role to the one after
        assertRingParticipants( member2, ID1, ID2, ID3 );
        member2.leaveRing();
        network.tickUntilDone();
        assertRingParticipants( member3, ID1, ID3 );
        
        // Rejoin
        member2.joinRing();
        network.tickUntilDone();
        assertRingParticipants( member2, ID1, ID2, ID3 );
        assertRingParticipants( member3, ID1, ID2, ID3 );
    }
    
    @Test
    public void startNewRingWith3ParticipantsAndSendTokenAround()
    {
        TestServer server1 = network.addServer( ID1 ).addStateTransitionListener( expectations.newExpectations()
                                                                                               .expect( TokenRingMessage.joinRing, joiningRing )
                                                                                               .expect( TokenRingMessage.ringDiscoveryTimedOut, master )
                                                                                               .expect( TokenRingMessage.sendToken, slave )
                                                                                               .build( ID1 ) );
        server1.newClient( TokenRing.class ).joinRing();
        
        network.tickUntilDone();

        TestServer server2 = network.addServer( ID2 ).addStateTransitionListener( expectations.newExpectations()
            .expect( TokenRingMessage.joinRing, joiningRing )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .expect( TokenRingMessage.sendToken, slave )
            .build( ID2 ) );
        server2.newClient( TokenRing.class ).joinRing();
        
        network.tickUntilDone();

        TestServer server3 = network.addServer( ID3 ).addStateTransitionListener( expectations.newExpectations()
            .expect( TokenRingMessage.joinRing, joiningRing )
            .expect( TokenRingMessage.ringDiscovered, slave )
            .expect( TokenRingMessage.becomeMaster, master )
            .build( ID3 ) );
        server3.newClient( TokenRing.class ).joinRing();
        
        network.tickUntilDone();

        TokenRing tokenRing1 = server1.newClient( TokenRing.class );
        tokenRing1.sendToken();

//        failures.nodeIsDown( ID2 );

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
                assertNotNull( me + " doesn't have any neighbors set", state.getNeighbours() );
                String before = state.getNeighbours().getBefore().getServerId();
                String after = state.getNeighbours().getAfter().getServerId();
                assertFalse( "Before and after both are '" + before + "', but was expected to be '" + one + "','" + other + "'", before.equals( after ) );
                assertTrue( "Expected one of '" + me + "'s neighbors to be '" + one + "', but was '" + before + "','" + after + "'", one.equals( before ) || one.equals( after ) );
                assertEquals( "Expected me to be '" + me + "', but was '" + state.getMe().getServerId() + "'", me, state.getMe().getServerId() );
                assertTrue( "Expected other of '" + me + "'s neighbors to be '" + one + "', but was '" + before + "','" + after + "'", other.equals( before ) || other.equals( after ) );
            }
        } );
    }
    
    private void verifyRingSizeTwoNeighbors( String me, String other )
    {
        verifyNeighbours( other, me, other );
        verifyNeighbours( me, other, me );
    }
    
    private void verifyRingSizeThreeNeighbors( String first, String second, String third )
    {
        verifyNeighboursWithUnknownPosition( second, first, third );
        verifyNeighboursWithUnknownPosition( first, second, third );
        verifyNeighboursWithUnknownPosition( first, third, second );
    }
    
    private TokenRing newMember( String id, Enum<?>... alternatingExpectedMessageAndState )
    {
        return newMember( id, false, alternatingExpectedMessageAndState );
    }
    
    @SuppressWarnings( "rawtypes" )
    private TokenRing newMember( String id, boolean includeUnchangedStates, Enum<?>... alternatingExpectedMessageAndState )
    {
        TestServer server = network.addServer( id );
        if ( alternatingExpectedMessageAndState.length > 0 )
        {
            ExpectationsBuilder expectationBuilder = expectations.newExpectations( alternatingExpectedMessageAndState );
            if ( includeUnchangedStates )
                expectationBuilder.includeUnchangedStates();
            server.addStateTransitionListener( expectationBuilder.build( id ) );
        }
        TokenRing member = server.newClient( TokenRing.class );
        member.joinRing();
        return member;
    }
    
    private void assertRingParticipants( TokenRing member,
            String... expectedParticipantIds ) throws InterruptedException, ExecutionException
    {
        Future<Iterable<RingParticipant>> participants = member.getParticipants();
        network.tickUntilDone();
        Set<String> expectedSet = new HashSet<String>( Arrays.asList( expectedParticipantIds ) );
        for ( RingParticipant participant : participants.get() )
            assertTrue( expectedSet.remove( participant.getServerId() ) );
        assertTrue( "Didn't get expected items: " + expectedSet, expectedSet.isEmpty() );
    }
}
