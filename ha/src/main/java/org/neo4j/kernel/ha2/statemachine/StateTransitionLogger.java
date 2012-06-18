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

package org.neo4j.kernel.ha2.statemachine;

import org.neo4j.com_2.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public class StateTransitionLogger
        implements StateTransitionListener
{
    private String participant;

    public StateTransitionLogger(String participant)
    {
        this.participant = participant;
    }

    public void stateTransition(StateTransition transition)
    {
        Logger logger = LoggerFactory.getLogger(transition.getOldState().getClass());

        if (logger.isInfoEnabled())
        {
            if( transition.getMessage().getPayload() instanceof String )
            {
                logger.info( participant + "/" +
                             transition.getOldState()
                                 .getClass()
                                 .getSuperclass()
                                 .getSimpleName() + ": " + transition.getOldState()
                    .toString() + "-[" + transition.getMessage().getMessageType() + ":" + transition.getMessage()
                    .getPayload() + "]->" +
                             transition.getNewState().toString() );
            }
            else
            {
                if( transition.getMessage().hasHeader( Message.FROM ) )
                {
                    logger.info( participant + "/" + transition.getOldState()
                        .getClass()
                        .getSuperclass()
                        .getSimpleName() + ": " + transition.getOldState().toString() + "-[" + transition.getMessage()
                        .getMessageType() + "(" + transition.getMessage()
                        .getHeader( Message.FROM ) + ")]->" + transition.getNewState().toString() );
                }
                else
                {
                    logger.info( participant + "/" + transition.getOldState()
                        .getClass()
                        .getSuperclass()
                        .getSimpleName() + ": " + transition.getOldState().toString() + "-[" + transition.getMessage()
                        .getMessageType() + "]->" + transition.getNewState().toString() );
                }
            }
        }
    }
}