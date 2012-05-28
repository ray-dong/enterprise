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
import org.neo4j.kernel.ha2.statemachine.State;

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
                    case addElectionListener:
                    {
                        context.addElectionListener(message.<ElectionListener>getPayload());
                        break;
                    }

                    case removeElectionListener:
                    {
                        context.removeElectionListener( message.<ElectionListener>getPayload() );
                        break;
                    }

                    case join:
                    {
                        context.created();

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
                    case addElectionListener:
                    {
                        context.addElectionListener(message.<ElectionListener>getPayload());
                        break;
                    }

                    case removeElectionListener:
                    {
                        context.removeElectionListener(message.<ElectionListener>getPayload());
                        break;
                    }

                    case demote:
                    {
                        URI demoteNode = message.getPayload();

                        context.clearSuggestions(demoteNode);

                        // Allow other live nodes to suggest which one should take over
                        for( URI uri : context.getClusterContext().getConfiguration().getNodes() )
                        {
                            if (!context.getHeartbeatContext().getFailed().contains( uri ))
                            {
                                // This is a candidate - allow it to suggest itself for promotion
                                outgoing.process( Message.to( ElectionMessage.suggest, uri, demoteNode ));
                            }
                        }
                        context.getClusterContext().timeouts.setTimeout( "demote-"+demoteNode, Message.timeout( ElectionMessage.suggestTimeout, message ) );
                        break;
                    }

                    case suggest:
                    {
                        URI demoteNode = message.getPayload();

                        if (context.getHeartbeatContext().isFailed( demoteNode ))
                        {
                            outgoing.process( Message.respond( ElectionMessage.suggestion, message, new ElectionMessage.SuggestionData( demoteNode, context
                                .getClusterContext()
                                .getConfiguration()
                                .getNodes()
                                .indexOf( context.getClusterContext().getMe() ))));
                        }
                        break;
                    }

                    case suggestion:
                    {
                        ElectionMessage.SuggestionData data = message.getPayload();
                        context.suggestion( data.getDemoteNode(), new URI( message.getHeader( Message.FROM )), data );

                        if (context.getSuggestions().get( data.getDemoteNode() ).size() == (context.getClusterContext().getConfiguration().getNodes().size()-context.getHeartbeatContext().getFailed().size()))
                        {
                            // We have all suggestions now
                            URI winner = context.getPreferredSuggestion(data.getDemoteNode());

                            // Broadcast this
                            for( String role : context.getRoles( data.getDemoteNode() ))
                            {
                                outgoing.process( Message.internal( AtomicBroadcastMessage.broadcast, new RoleElection( role, winner ) ) );
                            }
                        }
                        break;
                    }

                    case suggestTimeout:
                    {

                    }
                }

                return this;
            }
        }
}
