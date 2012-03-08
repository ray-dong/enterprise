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

package org.neo4j.com2.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Message for state machines which can be sent out in the cluster as well.
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
        return new Message(message, payload).setHeader( TO, BROADCAST );
    }

    public static Message to(MessageType message, Object to)
    {
        return to( message, to, null );
    }
    
    public static Message to(MessageType message, Object to, Object payload)
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
    
    public static final String BROADCAST = "*";

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
    
    public boolean isInternal()
    {
        return !headers.containsKey( Message.TO );
    }
    
    public boolean isBroadcast()
    {
        return !isInternal() && getHeader( Message.TO ).equals( BROADCAST );
    }
    
    public String getHeader(String name)
        throws IllegalArgumentException
    {
        String value = headers.get( name );
        if (value == null)
            throw new IllegalArgumentException( "No such header:"+name );
        return value;
    }

    public Message copyHeadersTo( Message message, String... names )
    {
        for( String name : names )
        {
            String value = headers.get( name );
            if (value != null)
                message.setHeader( name, value );
        }
        
        return message;
    }

    @Override
    public String toString()
    {
        return messageType.name()+headers+(payload != null && payload instanceof String ? ": "+payload : "");
    }
}
