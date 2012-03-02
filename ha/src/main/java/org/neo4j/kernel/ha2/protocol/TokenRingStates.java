/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.ha2.protocol;

import static org.neo4j.kernel.ha2.protocol.TokenRingMessages.becomeMaster;
import static org.neo4j.kernel.ha2.protocol.TokenRingMessages.discoverRing;
import static org.neo4j.kernel.ha2.protocol.TokenRingMessages.discoverRingFailed;
import static org.neo4j.kernel.ha2.protocol.TokenRingMessages.newAfter;
import static org.neo4j.kernel.ha2.protocol.TokenRingMessages.newBefore;
import static org.neo4j.kernel.ha2.protocol.TokenRingMessages.ringDiscovered;

import org.neo4j.kernel.ha2.protocol.context.RingNeighbours;
import org.neo4j.kernel.ha2.protocol.context.RingParticipant;
import org.neo4j.kernel.ha2.protocol.message.BroadcastMessage;
import org.neo4j.kernel.ha2.protocol.message.TargetedMessage;
import org.neo4j.kernel.ha2.protocol.statemachine.State;

/**
 * TODO
 */
public enum TokenRingStates
        implements State<TokenRingContext, TokenRingMessages>
{
    start
            {
                public State receive(TokenRingContext context, TokenRingMessages message, Object payLoad) throws Throwable
                {
                    switch(message)
                    {
                        case start:
                            context.broadcast(discoverRing);
                            context.expect(ringDiscovered, discoverRingFailed);
                            return initial;

                        default:
                            return this;
                    }
                }
            },

    initial
            {
                public State receive(TokenRingContext context, TokenRingMessages message, Object payLoad) throws Throwable
                {
                    switch (message)
                    {
                        case ringDiscovered:
                        {
                            TargetedMessage<RingNeighbours> event = (TargetedMessage<RingNeighbours>) payLoad;
                            context.setNeighbours(event.getPayload());
                            return slave;
                        }

                        case discoverRingFailed:
                        {
                            context.setNeighbours(context.getMe(), context.getMe());
                            return master;
                        }

                        case discoverRing:
                        {
                            BroadcastMessage<RingParticipant> event = (BroadcastMessage<RingParticipant>)payLoad;
                            if (event.getFrom().getServerId().compareTo(context.getMe().getServerId())>0)
                            {
                                // We're both looking for ring but this server has higher server id so wins
                                // and switches to master mode

                                context.setNeighbours(event.getFrom(),event.getFrom());
                                
                                context.send(ringDiscovered, event.getFrom(),
                                        new RingNeighbours(context.getMe(), context.getMe()));
                                
                                return master;
                            } else
                            {
                                return this;
                            }
                        }

                        default:
                            return this;
                    }
                }
            },

    master
            {
                public State receive(TokenRingContext context, TokenRingMessages message, Object payLoad) throws Throwable
                {
                    switch (message)
                    {
                        case discoverRing:
                        {
                            BroadcastMessage<RingParticipant> event = (BroadcastMessage<RingParticipant>)payLoad;
                            context.setNeighbours(event.getFrom(),
                                    context.getNeighbours().getAfter().equals(context.getMe())?event.getFrom(): context.getNeighbours().getAfter());

                            if (!context.getNeighbours().getBefore().equals(context.getMe()))
                                context.send(newAfter, context.getNeighbours().getBefore(), event.getFrom());
                            context.send(ringDiscovered, event.getFrom(),
                                    new RingNeighbours(context.getNeighbours().getBefore(), context.getMe()));

                            return this;
                        }

                        case newAfter:
                        {
                            TargetedMessage<RingParticipant> event = (TargetedMessage<RingParticipant>) payLoad;
                            context.newAfter(event.getPayload());
                            return this;
                        }

                        case newBefore:
                        {
                            TargetedMessage<RingParticipant> event = (TargetedMessage<RingParticipant>) payLoad;
                            context.newBefore(event.getPayload());
                            return this;
                        }

                        case leaveRing:
                        {
                            if (!context.getNeighbours().getAfter().equals(context.getMe()))
                            {
                                context.send(newAfter, context.getNeighbours().getBefore(), context.getNeighbours().getAfter());
                                context.send(becomeMaster, context.getNeighbours().getAfter(), context.getNeighbours().getBefore());
                                return start;
                            }
                        }

                        case sendToken:
                        {
                            context.send(becomeMaster, context.getNeighbours().getAfter(), null);
                            return slave;
                        }

                        default:
                            return this;
                    }
                }
            },

    slave
            {
                public State receive(TokenRingContext context, TokenRingMessages message, Object payLoad) throws Throwable
                {
                    switch (message)
                    {
                        case becomeMaster:
                        {
                            TargetedMessage<RingParticipant> event = (TargetedMessage<RingParticipant>) payLoad;
                            if (event.getPayload() != null)
                                context.newBefore(event.getPayload());

                            return master;
                        }

                        case newAfter:
                        {
                            TargetedMessage<RingParticipant> event = (TargetedMessage<RingParticipant>) payLoad;
                            context.newAfter(event.getPayload());
                            return this;
                        }

                        case newBefore:
                        {
                            TargetedMessage<RingParticipant> event = (TargetedMessage<RingParticipant>) payLoad;
                            context.newBefore(event.getPayload());

                            return this;
                        }

                        case leaveRing:
                        {
                            if (!context.getNeighbours().getAfter().equals(context.getMe()))
                            {
                                context.send(newAfter, context.getNeighbours().getBefore(), context.getNeighbours().getAfter());
                                context.send(newBefore, context.getNeighbours().getAfter(), context.getNeighbours().getBefore());
                            }

                            return start;
                        }

                        default:
                            return this;
                    }
                }
            }
}
