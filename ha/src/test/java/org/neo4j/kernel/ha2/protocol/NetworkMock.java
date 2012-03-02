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

import org.neo4j.kernel.ha2.protocol.context.RingParticipant;
import org.neo4j.kernel.ha2.protocol.message.BroadcastMessage;
import org.neo4j.kernel.ha2.protocol.message.TargetedMessage;
import org.neo4j.kernel.ha2.protocol.statemachine.*;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 */
public class NetworkMock
{
    Map<RingParticipant, StateMachine> participants = new HashMap<RingParticipant, StateMachine>();

    public void addParticipant(RingParticipant participant)
    {
        final TokenRingContext context = new TokenRingContext(participant);
        final StateMachine stateMachine = new StateMachine(context, TokenRingMessages.class, TokenRingStates.start);

        participants.put(participant, stateMachine);

        stateMachine.addStateTransitionListener(new StateTransitionLogger(participant));
        stateMachine.addStateTransitionListener(new StateTransitionListener()
        {
            public void stateTransition(State oldState, StateMessage event, State newState)
            {
                try
                {

                    while (!context.getSendQueue().isEmpty())
                        process(context.getSendQueue().poll());
                } catch (Throwable throwable)
                {
                    throwable.printStackTrace();
                }
            }
        });

        System.out.println("===="+participant+" joins ring");
        stateMachine.receive(new StateMessage("start"));
    }

    public void removeParticipant(RingParticipant participant)
    {
        System.out.println("===="+participant+" leaves ring");
        StateMachine stateMachine = participants.get(participant);
        stateMachine.receive(new StateMessage("leaveRing"));

        stateMachine = participants.remove(participant);
    }

    public void send(RingParticipant participant, String name)
    {
        send(participant, name, null);
    }

    public void send(RingParticipant participant, String name, Object payload)
    {
        System.out.println("===="+participant+":"+name);
        StateMachine targetMachine = participants.get(participant);
        targetMachine.receive(new StateMessage(name, payload));
    }

    private void process(StateMessage event)
    {
        Object payLoad = event.getPayload();

        if (payLoad instanceof BroadcastMessage)
        {
            BroadcastMessage broadcastEvent = (BroadcastMessage) payLoad;
            boolean alone = true;
            for (Map.Entry<RingParticipant, StateMachine> ringParticipantStateMachineEntry : participants.entrySet())
            {
                if (!ringParticipantStateMachineEntry.getKey().equals(broadcastEvent.getFrom()))
                {
                    alone = false;
                    ringParticipantStateMachineEntry.getValue().receive(event);
                }
            }

            if (alone)
            {
                participants.get(broadcastEvent.getFrom()).receive(new StateMessage(event.getName() + "Failed"));
            }
            return;
        }

        if (payLoad instanceof TargetedMessage)
        {
            TargetedMessage targetedEvent = (TargetedMessage) payLoad;
            StateMachine targetMachine = participants.get(targetedEvent.getTo());
            if (targetMachine == null)
            {
                System.out.println("Target "+targetedEvent.getTo()+" does not exist");
            } else
            {
                targetMachine.receive(event);
            }
            return;
        }

        System.out.println("Unknown payload type:"+payLoad.getClass().getName());
    }
}
