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
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos.ProposerState;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterMessage;
import org.neo4j.kernel.ha2.statemachine.State;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public enum ElectionState
    implements State<ElectionContext, ElectionMessage>
{
    start
        {
            @Override
            public State<?, ?> handle( ElectionContext context,
                                       Message<ElectionMessage> message,
                                       MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case created:
                    {
                        context.created();
                        return election;
                    }

                    case join:
                    {
                        return election;
                    }
                }

                return this;
            }
        },

    election
        {
            @Override
            public State<?, ?> handle( ElectionContext context,
                                       Message<ElectionMessage> message,
                                       MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case demote:
                    {
                        URI demoteNode = message.getPayload();

                        // Start election process for all roles that this node hade
                        for( String role : context.getRoles( demoteNode ) )
                        {
                            if (!context.isElectionProcessInProgress( role ))
                            {
                                context.startElectionProcess( role );

                                // Allow other live nodes to vote which one should take over
                                for( URI uri : context.getClusterContext().getConfiguration().getNodes() )
                                {
                                    if (!context.getHeartbeatContext().getFailed().contains( uri ))
                                    {
                                        // This is a candidate - allow it to vote itself for promotion
                                        outgoing.process( Message.to( ElectionMessage.vote, uri, role ));
                                    }
                                }
                                context.getClusterContext().timeouts.setTimeout( "election-"+role, Message.timeout( ElectionMessage.electionTimeout, message ) );
                            }
                        }

                        break;
                    }

                    case vote:
                    {
                        String role = message.getPayload();

/*
                        URI currentlyElectedNode = context.getClusterContext().getConfiguration().getElected( role );
                        if (currentlyElectedNode == null || context.nodeIsSuspected(currentlyElectedNode))
                        {
*/
                            outgoing.process( Message.respond( ElectionMessage.voted, message, new ElectionMessage.VotedData( role, context.getCredentialsForRole( role ))));
//                        }
                        break;
                    }

                    case voted:
                    {
                        ElectionMessage.VotedData data = message.getPayload();
                        context.voted( data.getRole(), new URI( message.getHeader( Message.FROM ) ), data.getVoteCredentials() );

                        if (context.getVoteCount(data.getRole()) == context.getNeededVoteCount())
                        {
                            context.getClusterContext().timeouts.cancelTimeout( "election-"+data.getRole() );

                            // We have all votes now
                            URI winner = context.getElectionWinner( data.getRole() );

                            // Broadcast this
                            ClusterMessage.ConfigurationChangeState configurationChangeState = new ClusterMessage.ConfigurationChangeState();
                            configurationChangeState.elected( data.getRole(), winner );
                            outgoing.process( Message.internal( ProposerMessage.propose,
                                                                configurationChangeState ));
                        }
                        break;
                    }

                    case electionTimeout:
                    {
                        // Something was lost
                        LoggerFactory.getLogger(getClass()).warn( "Election timed out" );
                    }
                }

                return this;
            }
        }
}
