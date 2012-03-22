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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.neo4j.com2.message.Message;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRing;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingContext;

/**
 * TODO
 */
public class NetworkMock
{
    Map<String, TestServer> participants = new HashMap<String, TestServer>();

    private final NetworkFailureStrategy failureStrategy;

    public NetworkMock()
    {
        this(new PerfectNetworkStrategy());
    }

    public NetworkMock( NetworkFailureStrategy failureStrategy )
    {
        this.failureStrategy = failureStrategy;
    }

    public TestServer addServer( String serverId )
    {
        TestServer server = new TestServer( serverId );

        debug( serverId, "joins ring" );

        participants.put( serverId, server );

        return server;
    }

    private void debug( String participant, String string )
    {
        Logger.getLogger("").info( "=== " + participant + " " + string );
    }

    public void removeServer( String serverId )
    {
        removeServer( serverId, true );
    }
    
    public void removeServer( String serverId, boolean callLeaveRing )
    {
        debug( serverId, "leaves ring" );
        TestServer server = participants.get(serverId);
        if ( callLeaveRing )
        {
            server.newClient( TokenRing.class ).leaveRing();
            tickUntilDone();
        }
        server.stop();

        participants.remove( serverId );
    }
    
    public int tick()
    {
        // Get all messages from all test servers
        List<Message> messages = new ArrayList<Message>(  );
        for( TestServer testServer : participants.values() )
        {
            testServer.sendMessages( messages );
        }
        
        // Now send them
        int nrOfReceivedMessages = 0;
        for( Message message : messages )
        {
            String to = message.getHeader( Message.TO );
            if ( to.equals( Message.BROADCAST ))
            {
                for( Map.Entry<String, TestServer> testServer : participants.entrySet() )
                {
                    if (!testServer.getKey().equals( message.getHeader( Message.FROM ) ))
                    {
                        if (failureStrategy.isLost( message, testServer.getKey() ))
                        {
                            Logger.getLogger("").info( "Broadcasted message to "+testServer.getKey()+" was lost");

                        } else
                        {
                            Logger.getLogger("").info( "Broadcast to "+testServer.getKey()+": "+message);
                            testServer.getValue().process( message );
                            nrOfReceivedMessages++;
                        }
                    }
                }
            } else
            {
                if (failureStrategy.isLost( message, to ))
                {
                    Logger.getLogger("").info( "Send message to "+to+" was lost");
                } else
                {
                    TestServer server = participants.get( to );
                    Logger.getLogger("").info( "Send to "+to+": "+message);
                    server.process( message );
                    nrOfReceivedMessages++;
                }
            }
        }
        return nrOfReceivedMessages;
    }
    
    public void tickUntilDone()
    {
        do
        {
            while (tick()>0){}
            
            for( TestServer testServer : participants.values() )
            {
                testServer.checkExpectations();
            }
        } while (tick() > 0);
    }
    
    public void verifyState( String serverId, Verifier<TokenRingContext> verifier )
    {
        TestServer participant = participants.get( serverId );
        if ( participant == null ) 
            throw new IllegalArgumentException( "Unknown server id '" + serverId + "'" );
        participant.verifyState( verifier );
    }

    public void visitServers( Visitor<Server> visitor )
    {
        for( TestServer testServer : participants.values() )
        {
            if (!visitor.visit( testServer.getServer() ))
                return;
        }
    }
}
