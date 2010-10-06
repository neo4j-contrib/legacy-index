/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.index.lucene;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

class LuceneIndexStore
{
    private long creationTime;
    private long randomIdentifier;
    private long version;
    
    private final FileChannel fileChannel;
    private final ByteBuffer buf = ByteBuffer.allocate( 24 );
    
    public LuceneIndexStore( String store )
    {
        if ( !new File( store ).exists() )
        {
            create( store );
        }
        try
        {
            fileChannel = new RandomAccessFile( store, "rw" ).getChannel();
            if ( fileChannel.read( buf ) != 24 )
            {
                throw new RuntimeException( "Expected to read 24 bytes" );
            }
            buf.flip();
            creationTime = buf.getLong();
            randomIdentifier = buf.getLong();
            version = buf.getLong();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    static void create( String store )
    {
        if ( new File( store ).exists() )
        {
            throw new IllegalArgumentException( store + " already exist" );
        }
        try
        {
            FileChannel fileChannel = 
                new RandomAccessFile( store, "rw" ).getChannel();
            ByteBuffer buf = ByteBuffer.allocate( 24 );
            long time = System.currentTimeMillis();
            long identifier = new Random( time ).nextLong();
            buf.putLong( time ).putLong( identifier ).putLong( 0 );
            buf.flip();
            if ( fileChannel.write( buf ) != 24 )
            {
                throw new RuntimeException( "Expected to write 24 bytes" );
            }
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getRandomNumber()
    {
        return randomIdentifier;
    }

    public long getVersion()
    {
        return version;
    }

    public synchronized long incrementVersion()
    {
        long current = getVersion();
        version++;
        writeOut();
        return current;
    }

    public synchronized void setVersion( long version )
    {
        this.version = version;
        writeOut();
    }
    
    private void writeOut()
    {
        buf.clear();
        buf.putLong( creationTime ).putLong( randomIdentifier ).putLong( 
            version );
        buf.flip();
        try
        {
            if ( fileChannel.write( buf, 0 ) != 24 )
            {
                throw new RuntimeException( "Expected to write 24 bytes" );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void close()
    {
        if ( !fileChannel.isOpen() )
        {
            return;
        }
        
        try
        {
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}