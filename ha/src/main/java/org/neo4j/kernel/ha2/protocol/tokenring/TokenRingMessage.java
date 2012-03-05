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

import org.neo4j.kernel.ha2.statemachine.message.MessageType;

/**
 * TODO
 */
public enum TokenRingMessage
    implements MessageType
{
    failure,
    ringDiscovered,
    discoverRing(failure, ringDiscovered),
    newAfter,newBefore,
    leaveRing,
    becomeMaster, sendToken,
    getParticipantsResponse,getParticipants(failure, getParticipantsResponse ),
    start;
    
    private TokenRingMessage failureMessage;
    private TokenRingMessage[] next;

    private TokenRingMessage()
    {
        next = new TokenRingMessage[0];
    }

    private TokenRingMessage( TokenRingMessage failureMessage, TokenRingMessage... next )
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
    public MessageType failureMessage()
    {
        return failureMessage;
    }


}
