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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;
import org.neo4j.com2.message.Message;
import org.neo4j.kernel.ha2.protocol.RingNeighbours;
import org.neo4j.kernel.ha2.protocol.RingParticipant;

/**
 * TODO
 */
public class TokenRingContext
{
    private Comparator<RingParticipant> participantComparator;

    private RingParticipant me;
    private RingNeighbours neighbours;

    List<RingParticipant> standbyMonitors = new ArrayList<RingParticipant>(  );
    private boolean hasToken = false;

    public TokenRingContext( Comparator<RingParticipant> participantComparator )
    {
        this.participantComparator = participantComparator;
    }

    public RingParticipant getMe()
    {
        return me;
    }

    public void setMe( RingParticipant me )
    {
        this.me = me;
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
        Logger.getAnonymousLogger().info( me + " has new neighbours: " + neighbours );
        this.neighbours = neighbours;
    }

    public void newBefore(RingParticipant before)
    {
        setNeighbours(new RingNeighbours(before, neighbours.getAfter()));
    }

    public void newAfter(RingParticipant after)
    {
        setNeighbours(new RingNeighbours(neighbours.getBefore(), after));
    }

    public boolean isConversationCreatedByMe( Message message )
    {
        return me.toString().equals(message.getHeader( Message.CREATED_BY ));
    }

    public boolean isAlone()
    {
        return neighbours.getAfter().equals(me);
    }

    // This is for the join protocol specifically
    public void addStandbyMonitor( RingParticipant from )
    {
        if (!standbyMonitors.contains( from ))
            standbyMonitors.add(from);
    }

    public List<RingParticipant> getStandbyMonitors()
    {
        return standbyMonitors;
    }

    public RingParticipant getRingInsertionPoint()
    {
        // TODO - if we have participated in ring before, we could
        // opt to insert at the same point as before, if possible

        RingParticipant insertionPoint = null;
        for( RingParticipant standbyMonitor : standbyMonitors )
        {
            if (participantComparator.compare( me, standbyMonitor ) > 0)
            {
                insertionPoint = standbyMonitor;
            }
        }

        if (insertionPoint == null && standbyMonitors.size() > 0)
        {
            insertionPoint = standbyMonitors.get( standbyMonitors.size()-1 );
        }

        return insertionPoint;
    }

    public void clearStandbyMonitors()
    {
        standbyMonitors.clear();
    }

    public Comparator<RingParticipant> getParticipantComparator()
    {
        return participantComparator;
    }
    
    // Token management
    public boolean hasToken()
    {
        return hasToken;
    }

    public void claimedToken()
    {
        hasToken = true;
    }

    public void sentToken()
    {
        hasToken = false;
    }
}