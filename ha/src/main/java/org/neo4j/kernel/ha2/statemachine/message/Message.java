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
package org.neo4j.kernel.ha2.statemachine.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingMessage;

/**
 * State machine message
 */
public class Message
    implements Serializable
{
    public static Message broadcast(MessageType message)
    {
        return broadcast(message, null);
    }

    public static Message broadcast(MessageType message, Object payload)
    {
        return new Message(message, payload).setHeader( TO, "*" );
    }

    public static Message to(MessageType message, RingParticipant to)
    {
        return to( message, to, null );
    }
    
    public static Message to(MessageType message, RingParticipant to, Object payload)
    {
        return new Message(message, payload).setHeader( TO, to.toString() );
    }
    
    public static Message internal(MessageType message, Object payload)
    {
        return new Message( message, payload );
    }
    
    // Standard headers
    public static final String CONVERSATION_ID = "conversation-id";
    public static final String CREATED_BY = "created-by";
    public static final String FROM = "from";
    public static final String TO = "to";

    final private MessageType messageType;
    final private Object payload;
    final private Map<String,String> headers = new HashMap<String, String>(  );

    protected Message( MessageType messageType, Object payload )
    {
        this.messageType = messageType;
        this.payload = payload;
    }

    public MessageType getMessageType()
    {
        return messageType;
    }

    public Object getPayload()
    {
        return payload;
    }
    
    public Message setHeader(String name, String value)
    {
        headers.put( name, value );
        return this;
    }
    
    public boolean hasHeader(String name)
    {
        return headers.containsKey( name );
    }
    
    public String getHeader(String name)
        throws IllegalArgumentException
    {
        String value = headers.get( name );
        if (value == null)
            throw new IllegalArgumentException( "No such header:"+name );
        return value;
    }

    public void copyHeadersTo( Message message, String... names )
    {
        for( String name : names )
        {
            String value = headers.get( name );
            if (value != null)
                message.setHeader( name, value );
        }
    }

    @Override
    public String toString()
    {
        return messageType.name()+headers;
    }
}
