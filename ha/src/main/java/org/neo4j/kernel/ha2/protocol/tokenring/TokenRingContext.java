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

package org.neo4j.kernel.ha2.protocol.tokenring;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.neo4j.kernel.ha2.protocol.RingNeighbours;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.statemachine.message.BroadcastMessage;
import org.neo4j.kernel.ha2.statemachine.message.InternalMessage;
import org.neo4j.kernel.ha2.statemachine.message.Message;
import org.neo4j.kernel.ha2.statemachine.message.TargetedMessage;

/**
 * TODO
 */
public class TokenRingContext
{
    private Queue<Message> sendQueue = new ConcurrentLinkedQueue<Message>();

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

    public Queue<Message> getSendQueue()
    {
        return sendQueue;
    }

    public void broadcast(TokenRingMessage message)
    {
        broadcast(message, null);
    }

    public void broadcast(TokenRingMessage message, Object payload)
    {
        sendQueue.add(new BroadcastMessage(message, me, payload));
    }

    public void send(TokenRingMessage message, RingParticipant to, Object payload)
    {
        sendQueue.add(new TargetedMessage(message, me, to, payload));
    }
    
    public void internal(TokenRingMessage message, Object payload)
    {
        sendQueue.add( new InternalMessage( message, payload ));
    }
}