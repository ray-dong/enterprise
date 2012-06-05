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

package org.neo4j.kernel.ha2.protocol.snapshot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.neo4j.com_2.message.MessageType;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public enum SnapshotMessage
    implements MessageType
{
    join,leave,
    setSnapshotProvider,
    refreshSnapshot,
    sendSnapshot, snapshot;

    public static class SnapshotState
        implements Serializable
    {
        private static byte[] sentBuf;
        private static byte[] receivedBuf;

        private long lastDeliveredInstanceId = -1;
        transient SnapshotProvider provider;

        transient byte[] buf;

        public SnapshotState( long lastDeliveredInstanceId, SnapshotProvider provider )
        {
            this.lastDeliveredInstanceId = lastDeliveredInstanceId;
            this.provider = provider;
        }

        public long getLastDeliveredInstanceId()
        {
            return lastDeliveredInstanceId;
        }

        public void setState(SnapshotProvider provider)
            throws IOException
        {
            ByteArrayInputStream bin = new ByteArrayInputStream( buf );
            ObjectInputStream oin = new ObjectInputStream( bin );
            try
            {
                provider.setState( oin );
            }
            catch( Throwable e )
            {
                e.printStackTrace();
            }
            finally
            {
                oin.close();
            }
        }

        private void writeObject( java.io.ObjectOutputStream out )
            throws IOException
        {
            out.defaultWriteObject();
            ByteArrayOutputStream bout = new ByteArrayOutputStream(  );
            ObjectOutputStream oout = new ObjectOutputStream( bout );
            provider.getState( oout );
            oout.close();
            byte[] buf = bout.toByteArray();
            out.writeInt( buf.length );
            out.write( buf );

            try
            {
                LoggerFactory.getLogger(getClass()).debug( new ObjectInputStream(new ByteArrayInputStream( buf )).readObject().toString() );
            }
            catch( ClassNotFoundException e )
            {
                e.printStackTrace();
            }
            sentBuf = buf;
        }

        private void readObject( java.io.ObjectInputStream in )
            throws IOException, ClassNotFoundException
        {
            in.defaultReadObject();
            buf = new byte[in.readInt()];
            in.readFully( buf );
            LoggerFactory.getLogger(getClass()).debug( "sent=received: " + new String( sentBuf ).equals( new String( buf ) ) );

        }
    }
}
