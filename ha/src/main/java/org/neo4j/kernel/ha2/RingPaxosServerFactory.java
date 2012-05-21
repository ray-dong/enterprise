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
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.CoordinatorMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.CoordinatorState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.RingPaxosContext;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.timeout.Timeouts;

import java.net.URI;

/**
 * TODO
 */
public class RingPaxosServerFactory
    implements ProtocolServerFactory
{
    @Override
    public ProtocolServer newProtocolServer(Timeouts timeouts, MessageSource input, MessageProcessor output )
    {
        final RingPaxosContext context = new RingPaxosContext();
        context.timeouts = timeouts;

        ConnectedStateMachines connectedStateMachines = new ConnectedStateMachines( input, output );

        StateMachine acceptor= new StateMachine(context, org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.AcceptorMessage.class, org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.AcceptorState.start);
        StateMachine coordinator= new StateMachine(context, CoordinatorMessage.class, CoordinatorState.start);
        StateMachine learner= new StateMachine(context, org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.LearnerMessage.class, org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.LearnerState.start);
        StateMachine paxosStateMachine = new StateMachine(context, org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.AtomicBroadcastMessage.class, org.neo4j.kernel.ha2.protocol.atomicbroadcast.ringpaxos.AtomicBroadcastState.start);

        connectedStateMachines.addStateMachine( acceptor );
        connectedStateMachines.addStateMachine( coordinator );
        connectedStateMachines.addStateMachine( learner );
        connectedStateMachines.addStateMachine( paxosStateMachine );

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