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
import org.neo4j.com2.message.MessageType;
import org.neo4j.helpers.collection.Visitor;

/**
 * TODO
 */
public abstract class NetworkMock<CONTEXT,MESSAGE extends Enum<MESSAGE>&MessageType, SERVER extends ProtocolServer<CONTEXT,MESSAGE> >
{
    Map<String, TestProtocolServer<CONTEXT,MESSAGE,SERVER>> participants = new HashMap<String, TestProtocolServer<CONTEXT,MESSAGE,SERVER>>();

    private final NetworkFailureStrategy failureStrategy;

    public NetworkMock()
    {
        this(new PerfectNetworkStrategy());
    }

    public NetworkMock( NetworkFailureStrategy failureStrategy )
    {
        this.failureStrategy = failureStrategy;
    }

    public TestProtocolServer<CONTEXT,MESSAGE,SERVER> addServer( String serverId )
    {
        TestProtocolServer<CONTEXT,MESSAGE,SERVER> server = newTestProtocolServer(serverId);

        debug( serverId, "joins ring" );

        participants.put( serverId, server );

        return server;
    }

    protected abstract TestProtocolServer<CONTEXT,MESSAGE,SERVER> newTestProtocolServer(String serverId);

    private void debug( String participant, String string )
    {
        Logger.getLogger("").info( "=== " + participant + " " + string );
    }

    public void removeServer( String serverId )
    {
        debug( serverId, "leaves ring" );
        TestProtocolServer server = participants.get(serverId);
        server.stop();

        participants.remove( serverId );
    }

    public int tick()
    {
        // Get all messages from all test servers
        List<Message> messages = new ArrayList<Message>(  );
        for( TestProtocolServer testServer : participants.values() )
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
                for( Map.Entry<String, TestProtocolServer<CONTEXT,MESSAGE,SERVER>> testServer : participants.entrySet() )
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
                    TestProtocolServer<CONTEXT,MESSAGE,SERVER> server = participants.get( to );
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
            
            for( TestProtocolServer testServer : participants.values() )
            {
                testServer.checkTimeouts();
            }
        } while (tick() > 0);
    }
    
    public void verifyState( String serverId, Verifier<CONTEXT> verifier )
    {
        TestProtocolServer participant = participants.get( serverId );
        if ( participant == null ) 
            throw new IllegalArgumentException( "Unknown server id '" + serverId + "'" );
        participant.verifyState( verifier );
    }

    public void visitServers( Visitor<SERVER> visitor )
    {
        for( TestProtocolServer<CONTEXT,MESSAGE,SERVER> testServer : participants.values() )
        {
            if (!visitor.visit( testServer.getServer() ))
                return;
        }
    }
}
