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

package org.neo4j.kernel.ha2.protocol.paxos;

import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

/**
 * TODO
 */
public enum ProposerState
    implements State<PaxosContext, PaxosMessage, ProposerState>
{
    start
        {
            @Override
            public ProposerState handle( PaxosContext paxosContext,
                                      Message<PaxosMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
/*
                switch( message.getMessageType() )
                {
                    case join:
                    {
                        outgoing.process( Message.broadcast( PaxosMessage.learn, "acceptors" ) );
                    }
                }
*/

                return this;
            }
        },

    proposer
        {
            @Override
            public ProposerState handle( PaxosContext paxosContext,
                                      Message<PaxosMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch(message.getMessageType())
                {
/*
                    case get:
                    {
                        // Send learn requests to all learners
                        for( String acceptor : paxosContext.getLearners() )
                        {
                            outgoing.process( Message.to( PaxosMessage.learn, acceptor, message.getPayload() ) );
                        }
                    }
                    
                    case learned:
                    {
                        outgoing.process( Message.internal( PaxosMessage.learned, message.getPayload() ) );
                    }
*/
                }
                
                return this;
            }
        },
}
