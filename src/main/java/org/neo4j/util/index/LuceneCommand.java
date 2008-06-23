package org.neo4j.util.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.neo4j.impl.transaction.xaframework.LogBuffer;
import org.neo4j.impl.transaction.xaframework.XaCommand;

abstract class LuceneCommand extends XaCommand
{
    private final long nodeId;
    private final String key;
    private final String value;
    
    private static final byte ADD_COMMAND = (byte) 1;
    private static final byte REMOVE_COMMAND = (byte) 2;
    
    LuceneCommand( long nodeId, String key, String value )
    {
        this.nodeId = nodeId;
        this.key = key;
        this.value = value;
    }
    
    LuceneCommand( CommandData data )
    {
        this.nodeId = data.nodeId;
        this.key = data.key;
        this.value = data.value;
    }
    
    public long getNodeId()
    {
        return nodeId;
    }
    
    public String getKey()
    {
        return key;
    }
    
    public String getValue()
    {
        return value;
    }
    
    static class AddCommand extends LuceneCommand
    {
        AddCommand( long nodeId, String key, String value )
        {
            super( nodeId, key, value );
        }
        
        AddCommand( CommandData data )
        {
            super( data );
        }
        
        @Override
        public void execute()
        {
            // TODO Auto-generated method stub
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( ADD_COMMAND );
            buffer.putLong( getNodeId() );
            char[] keyChars = getKey().toCharArray();
            buffer.putInt( keyChars.length );
            char[] valueChars = getValue().toCharArray();
            buffer.putInt( valueChars.length );
            buffer.put( keyChars );
            buffer.put( valueChars );
        }
    }
    
    static class RemoveCommand extends LuceneCommand
    {
        RemoveCommand( long nodeId, String key, String value )
        {
            super( nodeId, key, value );
        }
        
        RemoveCommand( CommandData data )
        {
            super( data );
        }
        
        @Override
        public void execute()
        {
            // TODO Auto-generated method stub
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( REMOVE_COMMAND );
            buffer.putLong( getNodeId() );
            char[] keyChars = getKey().toCharArray();
            buffer.putInt( keyChars.length );
            char[] valueChars = getValue().toCharArray();
            buffer.putInt( valueChars.length );
            buffer.put( keyChars );
            buffer.put( valueChars );
        }
    }

    private static class CommandData
    {
        private final long nodeId;
        private final String key;
        private final String value;
        
        CommandData( long nodeId, String key, String value )
        {
            this.nodeId = nodeId;
            this.key = key;
            this.value = value;
        }
    }
    
    static CommandData readCommandData( FileChannel fileChannel, 
        ByteBuffer buffer ) throws IOException
    {
        buffer.clear(); buffer.limit( 16 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        long nodeId = buffer.getLong();
        int keyCharLength = buffer.getInt();
        int valueCharLength = buffer.getInt();

        char[] keyChars = new char[keyCharLength];
        keyChars = readCharArray( fileChannel, buffer, keyChars );
        if ( keyChars == null )
        {
            return null;
        }
        String key = new String( keyChars );

        char[] valueChars = new char[valueCharLength];
        valueChars = readCharArray( fileChannel, buffer, valueChars );
        if ( valueChars == null )
        {
            return null;
        }
        String value = new String( valueChars );
        return new CommandData( nodeId, key, value );
    }
    
    private static char[] readCharArray( FileChannel fileChannel, 
        ByteBuffer buffer, char[] charArray ) throws IOException
    {
        buffer.clear();
        int bytesLeft = charArray.length * 2;
        int maxSize = buffer.capacity() / 2 * 2;
        int offset = 0; // offset in chars
        while ( bytesLeft > 0 )
        {
            if ( bytesLeft > maxSize )
            {
                buffer.limit( maxSize );
                bytesLeft -= maxSize;
            }
            else
            {
                buffer.limit( bytesLeft );
                bytesLeft = 0;
            }
            if ( fileChannel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            assert buffer.limit() % 2 == 0;
            int length = buffer.limit() / 2;
            buffer.asCharBuffer().get( charArray, offset, length ); 
            offset += length;
        }
        return charArray;
    }
    
    static XaCommand readCommand( FileChannel fileChannel, ByteBuffer buffer )
        throws IOException
    {
        buffer.clear(); buffer.limit( 1 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte commandType = buffer.get();
        CommandData data = readCommandData( fileChannel, buffer );
        if ( data == null )
        {
            return null;
        }
        switch ( commandType )
        {
            case ADD_COMMAND: return new AddCommand( data ); 
            case REMOVE_COMMAND: return new RemoveCommand( data );
            default:
                throw new IOException( "Unkown command type[" + 
                    commandType + "]" );
        }
    }
}
