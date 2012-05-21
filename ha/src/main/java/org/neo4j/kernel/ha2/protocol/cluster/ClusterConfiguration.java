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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: rickard
 * Date: 2012-05-16
 * Time: 14:46
 * To change this template use File | Settings | File Templates.
 */
public class ClusterConfiguration
{
    private final List<URI> nodes;
    private URI coordinator;
    private int allowedFailures = 1;

    public ClusterConfiguration(String... nodes) throws URISyntaxException
    {
        this.nodes = new ArrayList<URI>();
        for (String node : nodes)
        {
            this.nodes.add(new URI(node));
        }
    }

    public ClusterConfiguration(Collection<URI> nodes)
    {
        this.nodes = new ArrayList<URI>(nodes);
    }

    public void joined(URI nodeUrl)
    {
        if (nodes.contains(nodeUrl))
            return;

        nodes.add( nodeUrl );
    }

    public void left(URI nodeUrl)
    {
        nodes.remove(nodeUrl);
    }

    public void newCoordinator(URI nodeUri)
    {
        coordinator = nodeUri;
    }

    public boolean hasCoordinator()
    {
        return coordinator != null;
    }

    public void setNodes( List<URI> nodes )
    {
        this.nodes.clear();
        this.nodes.addAll(nodes);
    }

    public List<URI> getNodes()
    {
        return Collections.unmodifiableList(nodes);
    }

    public URI getCoordinator()
    {
        return coordinator;
    }

    public int getAllowedFailures()
    {
        return allowedFailures;
    }

    @Override
    public String toString()
    {
        return "Nodes:"+nodes;
    }

    public void left()
    {
        nodes.clear();
    }
}
