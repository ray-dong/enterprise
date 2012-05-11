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

import org.neo4j.com_2.message.MessageType;

/**
 * Learner state machine messages
 */
public enum LearnerMessage
    implements MessageType
{
    failure,
    join,leave,

    learn;

    public static class LearnState
        implements Serializable
    {
        private InstanceId instanceId;
        private Object value;

        public LearnState( InstanceId instanceId, Object value )
        {
            this.instanceId = instanceId;
            this.value = value;
        }

        public InstanceId getInstanceId()
        {
            return instanceId;
        }

        public Object getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return instanceId+": "+value;
        }
    }
}
