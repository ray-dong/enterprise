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
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.protocol.heartbeat.HeartbeatContext;

/**
 * TODO
 */
public class ElectionContext
{
    private List<ElectionRole> roles = new ArrayList<ElectionRole>(  );
    private ClusterContext clusterContext;
    private HeartbeatContext heartbeatContext;

    private Map<String, List<Vote>> votes = new HashMap<String, List<Vote>>(  );
    private ElectionCredentialsProvider electionCredentialsProvider;

    public ElectionContext( Iterable<ElectionRole> roles, ClusterContext clusterContext, HeartbeatContext heartbeatContext )
    {
        this.heartbeatContext = heartbeatContext;
        Iterables.addAll( this.roles, roles );
        this.clusterContext = clusterContext;
    }

    public void setElectionCredentialsProvider( ElectionCredentialsProvider electionCredentialsProvider )
    {
        this.electionCredentialsProvider = electionCredentialsProvider;
    }

    public void created()
    {
        for( ElectionRole role : roles )
        {
            // Elect myself for all roles
            clusterContext.elected( role.getName(), clusterContext.getMe() );
        }
    }

    public List<ElectionRole> getPossibleRoles()
    {
        return roles;
    }

    public Iterable<String> getRoles(URI server)
    {
        return clusterContext.getConfiguration().getRolesOf( server );
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

    public boolean isElectionProcessInProgress(String role)
    {
        return votes.containsKey( role );
    }

    public void startElectionProcess( String role )
    {
        List<Vote> voteList = new ArrayList<Vote>(  );
        votes.put( role, voteList );
    }

    public void voted( String role, URI suggestedNode, Comparable<Object> suggestionCredentials )
    {
        List<Vote> voteList = votes.get( role );
        if ( voteList == null)
            return; // We're not doing any election process for this role
        voteList.add( new Vote( suggestedNode, suggestionCredentials ) );
    }

    public URI getElectionWinner( String role, URI demoted )
    {
        List<Vote> voteList = votes.get( role );
        if ( voteList == null || voteList.isEmpty())
            return null;

        votes.remove( role );

        // Sort based on credentials
        // The most suited candidate should come out on top
        Collections.sort( voteList );
        Collections.reverse( voteList );

        for( Vote vote : voteList )
        {
            // Dont elect as winner the node we are trying to demote
            if (!vote.getSuggestedNode().equals( demoted ))
                return vote.getSuggestedNode();
        }

        return null;
    }

    public Comparable<Object> getCredentialsForRole( String role )
    {
        return electionCredentialsProvider.getCredentials( role );
    }

    public int getVoteCount( String role )
    {
        List<Vote> voteList = votes.get( role );
        if ( voteList == null)
            return 0;

        return voteList.size();
    }

    public int getNeededVoteCount()
    {
        return clusterContext.getConfiguration().getNodes().size()-heartbeatContext.getFailed().size();
    }

    public boolean nodeIsSuspected( URI node )
    {
        return heartbeatContext.getSuspicionsFor( clusterContext.getMe() ).contains( node );
    }

    private static class Vote
        implements Comparable<Vote>
    {
        private final URI suggestedNode;
        private final Comparable<Object> voteCredentials;

        private Vote( URI suggestedNode, Comparable<Object> voteCredentials )
        {
            this.suggestedNode = suggestedNode;
            this.voteCredentials = voteCredentials;
        }

        public URI getSuggestedNode()
        {
            return suggestedNode;
        }

        @Override
        public int compareTo( Vote o )
        {
            return o.voteCredentials.compareTo( voteCredentials );
        }
    }
}
