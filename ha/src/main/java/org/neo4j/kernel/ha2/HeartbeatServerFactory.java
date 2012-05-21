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

package org.neo4j.kernel.ha2;

import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.com_2.message.MessageSource;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartbeatState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.timeout.Timeouts;

import java.net.URI;

/**
 * TODO
 */
public class HeartbeatServerFactory
    implements ProtocolServerFactory
{
    @Override
    public ProtocolServer newProtocolServer(Timeouts timeouts, MessageSource input, MessageProcessor output )
    {
        final HeartbeatContext context = new HeartbeatContext(timeouts);

        ConnectedStateMachines connectedStateMachines = new ConnectedStateMachines( input, output );

        StateMachine failureDetection = new StateMachine(context, HeartbeatMessage.class, HeartbeatState.start);

        connectedStateMachines.addStateMachine( failureDetection );

        final ProtocolServer server = new ProtocolServer( connectedStateMachines );
        server.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                context.setMe( me.toString() );
            }
        } );

        return server;
    }
}