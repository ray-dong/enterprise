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

package org.neo4j.kernel.ha2.statemachine;

import java.lang.reflect.Proxy;

/**
 * TODO
 */
public class StateMachineProxyFactory
{
    private StateMachine stateMachine;
    private StateMachineConversations conversations;

    public StateMachineProxyFactory( StateMachine stateMachine, StateMachineConversations conversations )
    {
        this.stateMachine = stateMachine;
        this.conversations = conversations;
    }
    
    public <T> T newProxy(Class<T> proxyInterface)
    {
        stateMachine.checkValidProxyInterface( proxyInterface );

        return proxyInterface.cast( Proxy.newProxyInstance( proxyInterface.getClassLoader(), new Class<?>[]{ proxyInterface }, new StateMachineProxyHandler( stateMachine, conversations ) ) );
    }
}
