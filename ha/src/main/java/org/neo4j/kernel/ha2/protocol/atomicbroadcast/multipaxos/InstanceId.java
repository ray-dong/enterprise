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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos;

import java.io.Serializable;

/**
 * TODO
 */
public class InstanceId
    implements Serializable, Comparable<InstanceId>
{
    long id;

    public InstanceId( long id )
    {
        this.id = id;
    }

    public long getId()
    {
        return id;
    }

    @Override
    public int compareTo( InstanceId o )
    {
        return (int) (id - o.getId());
    }

    @Override
    public boolean equals( Object o )
    {
        if( this == o )
        {
            return true;
        }
        if( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InstanceId that = (InstanceId) o;

        if( id != that.id )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) ( id ^ ( id >>> 32 ) );
    }

    @Override
    public String toString()
    {
        return id+"";
    }
}
