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
package org.neo4j.kernel;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

public enum BranchedDataPolicy
{
    keep_all
    {
        @Override
        public void handle( GraphDatabaseAPI db )
        {
            moveAwayDb( db, newBranchedDataDir( db ) );
        }
    },
    keep_last
    {
        @Override
        public void handle( GraphDatabaseAPI db )
        {
            File branchedDataDir = newBranchedDataDir( db );
            moveAwayDb( db, branchedDataDir );
            for ( File file : getBranchedDataRootDirectory( db.getStoreDir() ).listFiles() )
            {
                if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                {
                    try
                    {
                        FileUtils.deleteRecursively( file );
                    }
                    catch ( IOException e )
                    {
                        db.getMessageLog().logMessage( "Couldn't delete old branched data directory " + file, e );
                    }
                }
            }
        }
    },
    keep_none
    {
        @Override
        public void handle( GraphDatabaseAPI db )
        {
            for ( File file : relevantDbFiles( db ) )
            {
                try
                {
                    FileUtils.deleteRecursively( file );
                }
                catch ( IOException e )
                {
                    db.getMessageLog().logMessage( "Couldn't delete file " + file, e );
                }
            }
        }
    },
    shutdown
    {
        @Override
        public void handle( GraphDatabaseAPI db )
        {
            db.shutdown();
        }
    };

    // Branched directories will end up in <dbStoreDir>/branched/<timestamp>/
    static String BRANCH_SUBDIRECTORY = "branched";

    public abstract void handle( GraphDatabaseAPI db );

    protected void moveAwayDb( GraphDatabaseAPI db, File branchedDataDir )
    {
        for ( File file : relevantDbFiles( db ) )
        {
            try
            {
                FileUtils.moveFileToDirectory( file, branchedDataDir );
            }
            catch ( IOException e )
            {
                db.getMessageLog().logMessage( "Couldn't move " + file.getPath() );
            }
        }
    }

    File newBranchedDataDir( GraphDatabaseAPI db )
    {
        File result = getBranchedDataDirectory( db.getStoreDir(), System.currentTimeMillis() );
        result.mkdirs();
        return result;
    }

    File[] relevantDbFiles( GraphDatabaseAPI db )
    {
        if (!new File( db.getStoreDir() ).exists())
            return new File[0];

        return new File( db.getStoreDir() ).listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File file )
            {
                return !file.getName().equals( StringLogger.DEFAULT_NAME ) && !isBranchedDataRootDirectory( file );
            }
        } );
    }

    public static boolean isBranchedDataRootDirectory( File directory )
    {
        return directory.isDirectory() && directory.getName().equals( BRANCH_SUBDIRECTORY );
    }
    
    public static boolean isBranchedDataDirectory( File directory )
    {
        return directory.isDirectory() && directory.getParentFile().getName().equals( BRANCH_SUBDIRECTORY ) &&
                isAllDigits( directory.getName() );
    }
    
    private static boolean isAllDigits( String string )
    {
        for ( char c : string.toCharArray() )
            if ( !Character.isDigit( c ) )
                return false;
        return true;
    }

    public static File getBranchedDataRootDirectory( String dbStoreDir )
    {
        return new File( dbStoreDir, BRANCH_SUBDIRECTORY );
    }
    
    public static File getBranchedDataDirectory( String dbStoreDir, long timestamp )
    {
        return new File( getBranchedDataRootDirectory( dbStoreDir ), "" + timestamp );
    }
    
    public static File[] listBranchedDataDirectories( String storeDir )
    {
        return getBranchedDataRootDirectory( storeDir ).listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File directory )
            {
                return isBranchedDataDirectory( directory );
            }
        } );
    }
}