/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.ha2.statemachine;

import java.io.Serializable;
import org.neo4j.kernel.ha2.statemachine.message.Message;

/**
 * TODO
 */
public class StateMessage
    implements Serializable
{
    private String conversationId;
    private Message message;

    public StateMessage(String conversationId, Message message)
    {
        this.conversationId = conversationId;
        this.message = message;
    }

    public String getConversationId()
    {
        return conversationId;
    }

    public Message getMessage()
    {
        return message;
    }

    public <T,E extends Enum> State<T, E> dispatch(T context, State<T,E> state)
            throws Throwable
    {
        return state.receive(context, message);
    }

    @Override
    public String toString()
    {
        return conversationId+"/"+message.getMessageType().name()+"/"+message.getPayload();
    }
}
