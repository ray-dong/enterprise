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

package org.neo4j.kernel.ha2.protocol.election;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatContext;

/**
 * TODO
 */
public class ElectionContext
{
    Iterable<ElectionListener> listeners = Listeners.newListeners();

    private List<ElectionRole> roles = new ArrayList<ElectionRole>(  );
    private ClusterContext clusterContext;
    private HeartbeatContext heartbeatContext;

    private Map<URI, List<Suggestion>> suggestions = new HashMap<URI, List<Suggestion>>(  );

    public ElectionContext( Iterable<ElectionRole> roles, ClusterContext clusterContext, HeartbeatContext heartbeatContext )
    {
        this.heartbeatContext = heartbeatContext;
        Iterables.addAll( this.roles, roles );
        this.clusterContext = clusterContext;
    }

    public void addElectionListener( ElectionListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeElectionListener(ElectionListener listener)
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    public void created()
    {
        for( ElectionRole role : roles )
        {
            // Elect myself for all roles
            clusterContext.elected( role.getName(), clusterContext.getMe() );
        }
    }

    public List<ElectionRole> getRoles()
    {
        return roles;
    }

    public Iterable<String> getRoles(URI server)
    {
        List<String> roles = new ArrayList<String>(  );
        for( Map.Entry<String, URI> stringURIEntry : clusterContext.getConfiguration().getRoles().entrySet() )
        {
            if (stringURIEntry.getValue().equals( server ))
                roles.add( stringURIEntry.getKey() );
        }
        return roles;
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public HeartbeatContext getHeartbeatContext()
    {
        return heartbeatContext;
    }

    public void unelect( String roleName )
    {
        clusterContext.getConfiguration().removeElected(roleName);
    }

    public void clearSuggestions( URI demoteNode )
    {
        suggestions.remove( demoteNode );
    }

    public void suggestion(URI demoteNode, URI suggestedNode, ElectionMessage.SuggestionData data)
    {
        List<Suggestion> suggestionList = (List<Suggestion>) suggestions.get( demoteNode );
        if (suggestionList == null)
        {
            suggestionList = new ArrayList<Suggestion>(  );
            suggestions.put( demoteNode, suggestionList );
        }
        suggestionList.add( new Suggestion( suggestedNode, data ) );
    }

    public Map<URI, List<Suggestion>> getSuggestions()
    {
        return suggestions;
    }

    public URI getPreferredSuggestion( URI demoteNode )
    {
        List<Suggestion> suggestionList = suggestions.get( demoteNode );
        Collections.sort( suggestionList );
        return suggestionList.get( 0 ).getSuggestedNode();
    }

    private static class Suggestion
        implements Comparable<Suggestion>
    {
        URI suggestedNode;
        ElectionMessage.SuggestionData data;

        private Suggestion( URI suggestedNode, ElectionMessage.SuggestionData data )
        {
            this.suggestedNode = suggestedNode;
            this.data = data;
        }

        public URI getSuggestedNode()
        {
            return suggestedNode;
        }

        @Override
        public int compareTo( Suggestion o )
        {
            return o.data.getIndex() - data.getIndex();
        }
    }
}
