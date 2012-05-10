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
import org.neo4j.com2.message.MessageType;

/**
 * Coordinator state machine messages
 */
public enum CoordinatorMessage
    implements MessageType
{
    failure,
    join,leave,
    phase1Failure,
    propose, // If no accept message is sent out, it means not enough promises have come in
    promise, // phase 1b
    denial, // phase 1b denial
    phase2Failure,
    accepted; // phase 2b

    public static class PromiseState
        implements Serializable
    {
        private long rnd;
        private long v_rnd;
        private Object v_val;

        public PromiseState( long rnd, long v_rnd, Object v_val )
        {
            this.rnd = rnd;
            this.v_rnd = v_rnd;
            this.v_val = v_val;
        }

        public long getRnd()
        {
            return rnd;
        }

        public long getV_rnd()
        {
            return v_rnd;
        }

        public Object getV_val()
        {
            return v_val;
        }
    }

    public static class DenialState
    {
        private long rnd;

        public DenialState( long rnd )
        {
            this.rnd = rnd;
        }

        public long getRnd()
        {
            return rnd;
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
