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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.com.Response;
import org.neo4j.com.TxExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class HaTxIdGenerator implements TxIdGenerator
{
    private final HaServiceSupplier stuff;
    private final int serverId;

    public HaTxIdGenerator( HaServiceSupplier stuff, int serverId )
    {
        this.stuff = stuff;
        this.serverId = serverId;
    }

    @Override
    public long generate( XaDataSource dataSource, int identifier )
    {
        Response<Long> response = stuff.getMaster().commitSingleResourceTransaction(
                stuff.getSlaveContext( dataSource ), dataSource.getName(),
                myPreparedTransactionToCommit( dataSource, identifier ) );
        stuff.receive( response );
        return response.response().longValue();
    }

    private TxExtractor myPreparedTransactionToCommit( final XaDataSource dataSource, final int identifier )
    {
        return new TxExtractor()
        {
            @Override
            public ReadableByteChannel extract()
            {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public void extract( LogBuffer buffer )
            {
                try
                {
                    dataSource.getPreparedTransaction( identifier, buffer );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    @Override
    public int getCurrentMasterId()
    {
        return stuff.getMasterServerId();
    }

    @Override
    public int getMyId()
    {
        return serverId;
    }
}