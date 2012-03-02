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

package org.neo4j.kernel.ha2.protocol.statemachine;

import java.io.Serializable;

/**
 * TODO
 */
public class StateMessage
    implements Serializable
{
    private String name;
    private Object payload;

    public StateMessage(String name)
    {
        this.name = name;
    }

    public StateMessage(String name, Object payload)
    {
        this.name = name;
        this.payload = payload;
    }

    public String getName()
    {
        return name;
    }

    public Object getPayload()
    {
        return payload;
    }

    public <T,E extends Enum> State<T, E> dispatch(Class<E> messageEnumType, T context, State<T,E> state)
            throws Throwable
    {
        E message = (E)Enum.valueOf(messageEnumType, name);
        return state.receive(context, message, payload);
    }

    @Override
    public String toString()
    {
        return name+"/"+payload;
    }
}
