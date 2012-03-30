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

import org.neo4j.com2.message.MessageType;

/**
 * TODO
 */
public enum TokenRingMessage2
    implements MessageType<TokenRingMessage2>
{
    failure,
    active_monitor_timed_out, standby_monitor_present, active_monitor_present(active_monitor_timed_out, standby_monitor_present), // Used to verify that ring is ok
    ring_purge,
    attachment_timed_out, attachment_timer (attachment_timed_out, active_monitor_present, standby_monitor_present,ring_purge),

    request_initialization_timed_out, initialize_station, change_parameters, request_initialization(request_initialization_timed_out, initialize_station),

    any_token_timed_out,any_token_timer(any_token_timed_out),

    claim_token,repeat_claim_token,

    getRingParticipantsResponse, getRingParticipants( failure, getRingParticipantsResponse ), getParticipants,
    joinRing;

    private TokenRingMessage2 failureMessage;
    private MessageType[] next;

    private TokenRingMessage2()
    {
        next = new TokenRingMessage2[0];
    }

    private TokenRingMessage2( TokenRingMessage2 failureMessage, MessageType... next )
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
    public TokenRingMessage2 failureMessage()
    {
        return failureMessage;
    }
}
