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

import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AcceptorState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.LearnerState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.MultiPaxosContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.timeout.Timeouts;

/**
 * TODO
 */
public class MultiPaxosServerFactory
    implements ProtocolServerFactory
{
    @Override
    public ProtocolServer newProtocolServer(Timeouts timeouts, MessageSource input, MessageProcessor output )
    {
        final MultiPaxosContext context = new MultiPaxosContext();
        context.timeouts = timeouts;

        ConnectedStateMachines connectedStateMachines = new ConnectedStateMachines( input, output );

        StateMachine paxosStateMachine= new StateMachine(context, AtomicBroadcastMessage.class, AtomicBroadcastState.start);
        StateMachine acceptor= new StateMachine(context, AcceptorMessage.class, AcceptorState.start);
        StateMachine coordinator= new StateMachine(context, ProposerMessage.class, ProposerState.start);
        StateMachine learner= new StateMachine(context, LearnerMessage.class, LearnerState.start);

        connectedStateMachines.addStateMachine( paxosStateMachine );
        connectedStateMachines.addStateMachine( acceptor );
        connectedStateMachines.addStateMachine( coordinator );
        connectedStateMachines.addStateMachine( learner );


        final ProtocolServer server = new ProtocolServer( connectedStateMachines );
        server.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( String me )
            {
                context.setMe( me );
            }
        } );

        return server;
    }
}
