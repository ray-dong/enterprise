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

import java.util.logging.Logger;
import org.neo4j.com2.NetworkNode;
import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartBeatContext;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartBeatMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartBeatState;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartBeatMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat.HeartBeatState;
import org.neo4j.kernel.ha2.statemachine.StateMachine;
import org.neo4j.kernel.ha2.statemachine.StateMachineConversations;
import org.neo4j.kernel.ha2.statemachine.StateMachineProxyFactory;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;

/**
 * TODO
 */
public class HeartBeatServer
    extends ProtocolServer<HeartBeatContext,HeartBeatMessage>
{
    protected final StateMachine<HeartBeatContext, HeartBeatMessage, HeartBeatState> failureDetection;

    public interface Configuration
        extends NetworkNode.Configuration
    {
    }

    private Logger logger = Logger.getLogger( getClass().getName() );

    public HeartBeatServer( HeartBeatContext context,
                            MessageSource input,
                            MessageProcessor output
    )
    {
        super(context, input, output);

        failureDetection = new StateMachine<HeartBeatContext, HeartBeatMessage, HeartBeatState>(context, HeartBeatMessage.class, HeartBeatState.start);

        connectedStateMachines.addMessageProcessor( new FromHeaderMessageProcessor() );
        connectedStateMachines.addStateMachine( failureDetection );
    }
    
    @Override
    public void start()
        throws Throwable
    {
    }

    public void listeningAt( String me )
    {
        logger.info( "=== " + me + " starts" );

        context.setMe( me );


        StateMachineConversations conversations = new StateMachineConversations(me);
        proxyFactory = new StateMachineProxyFactory<HeartBeatMessage>( me, HeartBeatMessage.class, failureDetection, connectedStateMachines, conversations );
        connectedStateMachines.addMessageProcessor( proxyFactory );

        super.listeningAt( me );
    }
    
    public void addStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        failureDetection.addStateTransitionListener( stateTransitionListener );
    }

    public <T> T newClient( Class<T> clientProxyInterface )
    {
        return proxyFactory.newProxy( clientProxyInterface );
    }

    private class FromHeaderMessageProcessor
        implements MessageProcessor
    {
        @Override
        public void process( Message message )
        {
            if( message.hasHeader( Message.TO ) )
            {
                message.setHeader( Message.FROM, context.me );
            }
        }
    }
}
