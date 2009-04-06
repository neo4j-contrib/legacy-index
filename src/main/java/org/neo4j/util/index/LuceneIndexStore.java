package org.neo4j.util.index;

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
