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

import static java.lang.Integer.parseInt;

import java.net.URI;
import java.util.Map;

import org.neo4j.com_2.NetworkNode;
import org.neo4j.kernel.ha2.timeout.TimeoutStrategy;
import org.neo4j.kernel.ha2.timeout.Timeouts;
import org.neo4j.kernel.ha2.timeout.TimeoutsService;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;

/**
 * TODO
 */
public class NetworkedServerFactory
{
    private LifeSupport life;
    private ProtocolServerFactory protocolServerFactory;
    private TimeoutStrategy timeoutStrategy;
    private StringLogger logger;

    public NetworkedServerFactory( LifeSupport life, ProtocolServerFactory protocolServerFactory, TimeoutStrategy timeoutStrategy, StringLogger logger )
    {
        this.life = life;
        this.protocolServerFactory = protocolServerFactory;
        this.timeoutStrategy = timeoutStrategy;
        this.logger = logger;
    }

    public ProtocolServer newNetworkedServer(final Map<String, String> configuration)
    {
        NetworkNode.Configuration config = new NetworkNode.Configuration()
        {
            @Override
            public int[] port( int[] defaultPortRange, int min, int max )
            {
                 String port = configuration.get( "port" );
                 return port != null ? new int[] { parseInt( port ), parseInt( port ) } : defaultPortRange;
            }
            
            @Override
            public String address( String def )
            {
                return configuration.containsKey( "address" ) ? configuration.get( "address" ) : def;
            }
        };
        final NetworkNode node = new NetworkNode( config, logger );
        life.add( node );

        Timeouts timeouts = life.add(new TimeoutsService( timeoutStrategy, node ));

        final ProtocolServer protocolServer = protocolServerFactory.newProtocolServer(timeouts, node, node);
        node.addNetworkChannelsListener( new NetworkNode.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                protocolServer.listeningAt( me );
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
        life.add( protocolServer );
        
        return protocolServer;
    }
}
