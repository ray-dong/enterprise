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

package org.neo4j.kernel.ha2.protocol.paxos;

import org.neo4j.com2.message.MessageType;

/**
 * TODO
 */
public enum PaxosMessage
    implements MessageType<PaxosMessage>
{
    failure,
    joined, join(failure, joined),left, leave(failure, left),
    set,get,exists;

    private PaxosMessage failureMessage;
    private MessageType[] next;

    private PaxosMessage()
    {
        next = new PaxosMessage[0];
    }

    private PaxosMessage( PaxosMessage failureMessage, MessageType... next )
    {
        this.failureMessage = failureMessage;
        this.next = next;
    }

    @Override
    public MessageType[] next()
    {
        return next;
    }

    @Override
    public PaxosMessage failureMessage()
    {
        return failureMessage;
    }
}
