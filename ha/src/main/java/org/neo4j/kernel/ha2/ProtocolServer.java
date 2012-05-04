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
import org.neo4j.com2.message.MessageType;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.ha2.statemachine.StateMachineProxyFactory;
import org.neo4j.kernel.ha2.statemachine.StateTransitionListener;

/**
 * TODO
 */
public abstract class ProtocolServer<CONTEXT,MESSAGE extends Enum<MESSAGE>&MessageType>
    implements Lifecycle
{
    protected final CONTEXT context;
    protected StateMachineProxyFactory<MESSAGE> proxyFactory;
    protected final ConnectedStateMachines connectedStateMachines;
    private String me;

    public interface Configuration
        extends NetworkNode.Configuration
    {
    }

    private Logger logger = Logger.getLogger( getClass().getName() );

    public ProtocolServer( CONTEXT context,
                           MessageSource input,
                           MessageProcessor output
    )
    {
        this.context = context;
        connectedStateMachines = new ConnectedStateMachines( input, output );

        connectedStateMachines.addMessageProcessor( new FromHeaderMessageProcessor() );
    }
    
    @Override
    public void init()
        throws Throwable
    {
    }

    @Override
    public void stop()
        throws Throwable
    {
        logger.info( "Stop server" );
    }

    @Override
    public void shutdown()
        throws Throwable
    {
    }

    public void listeningAt( String me )
    {
        this.me = me;
    }

    /**
     * Ok to have this accessible like this?
     * @return server id
     */
    public String getServerId()
    {
        return me;
    }

    public CONTEXT getContext()
    {
        return context;
    }

    public abstract void addStateTransitionListener( StateTransitionListener stateTransitionListener );

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
                message.setHeader( Message.FROM, me );
            }
        }
    }
}
