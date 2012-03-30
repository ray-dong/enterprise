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

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.com2.NetworkNode;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.com2.message.MessageSource;
import org.neo4j.kernel.ConfigProxy;
import org.neo4j.kernel.LifeSupport;
import org.neo4j.kernel.ha2.failure.AbstractMessageFailureHandler;
import org.neo4j.kernel.ha2.failure.TimeoutMessageFailureHandler;
import org.neo4j.kernel.ha2.protocol.RingParticipant;
import org.neo4j.kernel.ha2.protocol.tokenring.ServerIdRingParticipantComparator;
import org.neo4j.kernel.ha2.protocol.tokenring.TokenRingContext;
import org.neo4j.kernel.ha2.timeout.FixedTimeoutStrategy;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class NetworkedServerFactory
{
    private LifeSupport life;

    public NetworkedServerFactory( LifeSupport life )
    {
        this.life = life;
    }

    public Server newNetworkedServer(Map<String, String> configuration)
    {
        NetworkNode.Configuration config = ConfigProxy.config( configuration, NetworkNode.Configuration.class );
        NetworkNode node = new NetworkNode( config, StringLogger.SYSTEM );
        life.add( node );

        final Server server = new Server( new TokenRingContext(new ServerIdRingParticipantComparator()), node, node, new AbstractMessageFailureHandler.Factory()
        {
            @Override
            public AbstractMessageFailureHandler newMessageFailureHandler( MessageProcessor incoming,
                                                                           MessageSource outgoing,
                                                                           MessageSource source
            )
            {
                return life.add(new TimeoutMessageFailureHandler( incoming, outgoing, source,
                                                           new FixedTimeoutStrategy(TimeUnit.SECONDS.toMillis( 2 )) ));
            }
        });
        node.addNetworkChannelsListener( new NetworkNode.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                RingParticipant participant = new RingParticipant( me.toString() );
                server.listeningAt( participant );
            }

            @Override
            public void channelOpened( URI to )
            {
            }

            @Override
            public void channelClosed( URI to )
            {
            }
        } );
        life.add( server );
        
        return server;
    }
}
