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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast;

import org.neo4j.helpers.Specification;

/**
 * Use this to filter out values of a particular type. Many clients will use the same AtomicBroadcast API, so this
 * can help separate them apart, based on types.
 */
public class AtomicBroadcastListenerFilter
    implements AtomicBroadcastListener
{
    Specification<Object> filter;
    private AtomicBroadcastListener delegate;

    public AtomicBroadcastListenerFilter( final Class<?> valueType, AtomicBroadcastListener delegate)
    {
        this.delegate = delegate;
        filter = new Specification<Object>()
        {
            @Override
            public boolean satisfiedBy( Object item )
            {
                return valueType.isInstance( item );
            }
        };
    }

    @Override
    public void receive( Object value )
    {
        if (filter.satisfiedBy( value ))
            delegate.receive( value );
    }
}
