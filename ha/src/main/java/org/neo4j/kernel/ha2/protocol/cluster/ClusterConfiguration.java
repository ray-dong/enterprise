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

package org.neo4j.kernel.ha2.protocol.cluster;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Specification;
import org.neo4j.helpers.collection.Iterables;

/**
 * Cluster configuration. Includes name of cluster, list of nodes, and role mappings
 *
 */
public class ClusterConfiguration
{
    public static final String COORDINATOR = "coordinator";

    private final String name;
    private List<URI> nodes;
    private Map<String, URI> roles = new HashMap<String, URI>();
    private int allowedFailures = 1;

    public ClusterConfiguration(String name, String... nodes)
    {
        this.name = name;
        this.nodes = new ArrayList<URI>();
        for (String node : nodes)
        {
            try
            {
                this.nodes.add(new URI(node));
            }
            catch( URISyntaxException e )
            {
                e.printStackTrace();
            }
        }
    }

    public ClusterConfiguration(String name, Collection<URI> nodes)
    {
        this.name = name;
        this.nodes = new ArrayList<URI>(nodes);
    }

    public ClusterConfiguration(ClusterConfiguration copy)
    {
        this.name = copy.name;
        this.nodes = new ArrayList<URI>( copy.nodes );
        this.roles = new HashMap<String, URI>( copy.roles );
    }

    public void joined( URI nodeUrl )
    {
        if (nodes.contains(nodeUrl))
            return;

        this.nodes = new ArrayList<URI>(nodes);
        nodes.add( nodeUrl );
    }

    public void left(URI nodeUrl)
    {
        this.nodes = new ArrayList<URI>(nodes);
        nodes.remove( nodeUrl );

        // Remove any roles that this node had
        Iterator<Map.Entry<String,URI>> entries = roles.entrySet().iterator();
        while( entries.hasNext() )
        {
            Map.Entry<String, URI> roleEntry = entries.next();

            if (roleEntry.getValue().equals( nodeUrl ))
                entries.remove();
        }
    }

    public void elected( String name, URI node )
    {
        roles = new HashMap<String, URI>( roles );
        roles.put( name, node );
    }

    public void setNodes( Iterable<URI> nodes )
    {
        this.nodes = new ArrayList<URI>();
        for( URI node : nodes )
        {
            this.nodes.add( node );
        }
    }

    public void setRoles( Map<String, URI> roles )
    {
        this.roles.clear();
        this.roles.putAll( roles );
    }

    public List<URI> getNodes()
    {
        return nodes;
    }

    public String getName()
    {
        return name;
    }

    public Map<String, URI> getRoles()
    {
        return roles;
    }

    public int getAllowedFailures()
    {
        return allowedFailures;
    }

    public void left()
    {
        this.nodes = new ArrayList<URI>();
    }

    public void removeElected( String roleName )
    {
        roles = new HashMap<String, URI>( roles );
        roles.remove( roleName );
    }

    public URI getElected( String roleName )
    {
        return roles.get( roleName );
    }

    public Iterable<String> getRoles( final URI node)
    {
        return Iterables.map( new Function<Map.Entry<String, URI>, String>()
        {
            @Override
            public String map( Map.Entry<String, URI> stringURIEntry )
            {
                return stringURIEntry.getKey();
            }
        }, Iterables.filter( new Specification<Map.Entry<String, URI>>()
        {
            @Override
            public boolean satisfiedBy( Map.Entry<String, URI> item )
            {
                return item.getValue().equals( node );
            }
        }, roles.entrySet() ) );
    }

    @Override
    public String toString()
    {
        return "Nodes:"+nodes+" Roles:"+roles;
    }
}
