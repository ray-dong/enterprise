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
import java.util.List;
import java.util.logging.Logger;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.StateMachine;

import static org.neo4j.com2.message.Message.*;

/**
 * TODO
 */
public class NetworkedStateMachine
    implements Lifecycle, MessageProcessor, MessageSource
{
    private Logger logger = Logger.getLogger( NetworkedStateMachine.class.getName() );
    
    private RingParticipant me;
    private final MessageSource source;
    private MessageProcessor sender;
    private StateMachine stateMachine;

    private List<Message> outgoingMessages = new ArrayList<Message>();
    private List<MessageProcessor> outgoingProcessors = new ArrayList<MessageProcessor>(  );
    protected final MessageProcessor outgoing;

    public NetworkedStateMachine( MessageSource source,
                                  final MessageProcessor sender,
                                  final StateMachine stateMachine
    )
    {
        this.source = source;
        this.sender = sender;
        this.stateMachine = stateMachine;

        outgoing = new OutgoingMessageProcessor();
    }

    @Override
    public void init()
        throws Throwable
    {
        source.addMessageProcessor( this );
    }

    @Override
    public void start()
        throws Throwable
    {
        logger.info( "=== " + me + " starts" );
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
    }
    
    public void setMe(RingParticipant participant)
    {
        this.me = participant;
    }

    @Override
    public void addMessageProcessor( MessageProcessor messageProcessor )
    {
        outgoingProcessors.add( messageProcessor );
    }

    @Override
    public synchronized void process( Message message )
    {
        stateMachine.handle( message, outgoing );

        // Process and send messages
        for( Message outgoingMessage : outgoingMessages )
        {
            message.copyHeadersTo( outgoingMessage, CONVERSATION_ID, CREATED_BY );

            for( MessageProcessor outgoingProcessor : outgoingProcessors )
            {
                try
                {
                    outgoingProcessor.process( outgoingMessage );
                }
                catch( Throwable e )
                {
                    e.printStackTrace();
                }
            }

            if( outgoingMessage.hasHeader( Message.TO ) )
            {
                outgoingMessage.setHeader( Message.FROM, me.getServerId() );

                try
                {
                    sender.process( outgoingMessage );
                }
                catch( Throwable e )
                {
                    e.printStackTrace();
                }
            }
        }
        outgoingMessages.clear();
    }

    private class OutgoingMessageProcessor
        implements MessageProcessor
    {
        @Override
        public void process( Message message )
        {
            outgoingMessages.add( message );
        }
    }
}
