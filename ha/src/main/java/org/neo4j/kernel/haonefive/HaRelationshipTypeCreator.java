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
package org.neo4j.kernel.haonefive;

import javax.transaction.TransactionManager;

import org.neo4j.com.Response;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class HaRelationshipTypeCreator implements RelationshipTypeCreator
{
    private final HaServiceSupplier stuff;

    public HaRelationshipTypeCreator( HaServiceSupplier stuff )
    {
        this.stuff = stuff;
    }
    
    @Override
    public int getOrCreate( TransactionManager txManager, EntityIdGenerator idGenerator,
            PersistenceManager persistence, RelationshipTypeHolder relTypeHolder, String name )
    {
        Response<Integer> response = stuff.getMaster().createRelationshipType( stuff.getSlaveContext(), name );
        stuff.receive( response );
        return response.response().intValue();
    }
}
