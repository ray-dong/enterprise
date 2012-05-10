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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos;

import java.io.Serializable;
import java.util.List;
import org.neo4j.com_2.message.MessageType;

/**
 * Acceptor state machine messages
 */
public enum AcceptorMessage
    implements MessageType
{
    failure,
    join,leave,
    prepare, // phase 1a/1b
    accept, // phase 2a/2b - timeout if accepted is not received fast enough
    accepted;

    public static class PrepareState
        implements Serializable
    {
        private long c_rnd;
        private List<String> c_ring;

        public PrepareState( long c_rnd, List<String> c_ring )
        {
            this.c_rnd = c_rnd;
            this.c_ring = c_ring;
        }

        public long getC_rnd()
        {
            return c_rnd;
        }

        public List<String> getC_ring()
        {
            return c_ring;
        }
    }

    public static class AcceptState
        implements Serializable
    {
        private long c_rnd;
        private Object c_val;
        private int c_vid;

        public AcceptState( long c_rnd, Object c_val, int c_vid )
        {
            this.c_rnd = c_rnd;
            this.c_val = c_val;
            this.c_vid = c_vid;
        }

        public long getC_rnd()
        {
            return c_rnd;
        }

        public Object getC_val()
        {
            return c_val;
        }

        public int getC_vid()
        {
            return c_vid;
        }
    }

    public static class AcceptedState
        implements Serializable
    {
        private long v_rnd;
        private int c_vid;

        public AcceptedState( long v_rnd, int c_vid )
        {
            this.v_rnd = v_rnd;
            this.c_vid = c_vid;
        }

        public long getV_rnd()
        {
            return v_rnd;
        }

        public int getC_vid()
        {
            return c_vid;
        }
    }
}
