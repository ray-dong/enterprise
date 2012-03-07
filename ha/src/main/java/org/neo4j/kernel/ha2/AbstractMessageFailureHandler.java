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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.com2.message.MessageType;

import static org.neo4j.com2.message.Message.*;

/**
 * TODO
 */
public class AbstractMessageFailureHandler
{
    private MessageProcessor incoming;
    protected Map<String, ExpectationFailure> expectations;

    public AbstractMessageFailureHandler( MessageProcessor incoming, MessageSource outgoing, MessageSource source )
    {
        expectations = new ConcurrentHashMap<String, ExpectationFailure>();
        this.incoming = incoming;
        source.addMessageProcessor( new CancelTimeouts() );
        outgoing.addMessageProcessor( new ExpectationTimeouts() );
    }
    
    protected void expectation(ExpectationFailure expectationFailure)
    {
        expectations.put( expectationFailure.getMessage().getHeader( Message.CONVERSATION_ID ), expectationFailure );
    }
    
    class ExpectationTimeouts
        implements MessageProcessor
    {
        @Override
        public void process( Message message )
        {
            if (message.hasHeader( Message.TO ))
            {
                MessageType messageType = message.getMessageType();
                if( messageType.failureMessage() != null )
                {
                    ExpectationFailure expectationFailure = new ExpectationFailure( message );
                    
                    expectation( expectationFailure );
                }
            }
        }
    }

    class CancelTimeouts
        implements MessageProcessor
    {
        @Override
        public void process( Message message )
        {
            ExpectationFailure expectationFailure = expectations.remove( message.getHeader( CONVERSATION_ID ) );
            if( expectationFailure != null )
            {
                expectationFailure.cancel();
            }
        }
    }

    class ExpectationFailure
        implements Runnable
    {
        private boolean cancelled = false;
        private Message message;

        public ExpectationFailure( Message message )
        {
            this.message = message;
        }

        public Message getMessage()
        {
            return message;
        }

        public synchronized void cancel()
        {
            cancelled = true;
        }

        @Override
        public synchronized void run()
        {
            expectations.remove( message.getHeader( CONVERSATION_ID ) );

            if( !cancelled )
            {
                incoming.process( message.copyHeadersTo( Message.internal( message.getMessageType()
                                                                               .failureMessage(), "Timed out" ), CONVERSATION_ID ) );
            }
        }
    }
}
