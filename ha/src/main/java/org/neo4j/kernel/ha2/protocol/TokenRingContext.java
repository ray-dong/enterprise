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

package org.neo4j.kernel.ha2.protocol;

import org.neo4j.kernel.ha2.protocol.context.RingNeighbours;
import org.neo4j.kernel.ha2.protocol.context.RingParticipant;
import org.neo4j.kernel.ha2.protocol.message.BroadcastMessage;
import org.neo4j.kernel.ha2.protocol.message.ExpectationMessage;
import org.neo4j.kernel.ha2.protocol.message.TargetedMessage;
import org.neo4j.kernel.ha2.protocol.statemachine.StateMessage;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * TODO
 */
public class TokenRingContext
{
    private Queue<StateMessage> sendQueue = new ConcurrentLinkedQueue<StateMessage>();

    private RingParticipant me;
    private RingNeighbours neighbours;

    public TokenRingContext(RingParticipant me)
    {
        this.me = me;
    }

    public RingParticipant getMe()
    {
        return me;
    }

    public RingNeighbours getNeighbours()
    {
        return neighbours;
    }

    public void setNeighbours(RingParticipant before, RingParticipant after)
    {
        setNeighbours(new RingNeighbours(before, after));
    }

    public void setNeighbours(RingNeighbours neighbours)
    {
        this.neighbours = neighbours;
    }

    public void newBefore(RingParticipant before)
    {
        neighbours = new RingNeighbours(before, neighbours.getAfter());
    }

    public void newAfter(RingParticipant after)
    {
        neighbours = new RingNeighbours(neighbours.getBefore(), after);
    }

    public Queue<StateMessage> getSendQueue()
    {
        return sendQueue;
    }

    public void broadcast(TokenRingMessages message)
    {
        broadcast(message, null);
    }

    public void broadcast(TokenRingMessages message, Object payload)
    {
        sendQueue.add(new StateMessage(message.name(), new BroadcastMessage<Object>(me, payload)));
    }

    public void send(TokenRingMessages message, RingParticipant to, Object payload)
    {
        sendQueue.add(new StateMessage(message.name(), new TargetedMessage<Object>(me, to, payload)));
    }

    public void expect(TokenRingMessages expected, TokenRingMessages failMessage)
    {
        sendQueue.add(new StateMessage(expected.name(), new ExpectationMessage(failMessage)));
    }
}