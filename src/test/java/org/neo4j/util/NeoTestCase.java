package org.neo4j.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Transaction;

/**
 * Base class for the meta model tests.
 */
public abstract class NeoTestCase extends TestCase
{
	private File basePath = new File( "var/test" );
    private File neoPath = new File( basePath, "neo" );
    private NeoService neo;
    private Transaction tx;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        deleteFileOrDirectory( neoPath );
        neo = new EmbeddedNeo( neoPath.getAbsolutePath() );
        tx = neo.beginTx();
    }
    
    @Override
    protected void tearDown() throws Exception
    {
        tx.success();
        tx.finish();
        beforeNeoShutdown();
        neo.shutdown();
        super.tearDown();
    }
    
    protected void beforeNeoShutdown()
    {
    }
    
    protected File getBasePath()
    {
        return basePath;
    }
    
    protected void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }
        
        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }

    protected void restartTx()
    {
        restartTx( true );
    }
    
    protected void restartTx( boolean success )
    {
        if ( success )
        {
            tx.success();
        }
        else
        {
            tx.failure();
        }
        tx.finish();
        tx = neo.beginTx();
    }

    protected NeoService neo()
    {
        return neo;
    }
    
    protected <T> void assertCollection( Collection<T> collection, T... items )
    {
        String collectionString = join( ", ", collection.toArray() );
        assertEquals( collectionString, items.length, collection.size() );
        for ( T item : items )
        {
            assertTrue( collection.contains( item ) );
        }
    }

    protected <T> Collection<T> asCollection( Iterable<T> iterable )
    {
        List<T> list = new ArrayList<T>();
        for ( T item : iterable )
        {
            list.add( item );
        }
        return list;
    }

    protected <T> String join( String delimiter, T... items )
    {
        StringBuffer buffer = new StringBuffer();
        for ( T item : items )
        {
            if ( buffer.length() > 0 )
            {
                buffer.append( delimiter );
            }
            buffer.append( item.toString() );
        }
        return buffer.toString();
    }

    protected <T> int countIterable( Iterable<T> iterable )
    {
        int counter = 0;
        Iterator<T> itr = iterable.iterator();
        while ( itr.hasNext() )
        {
            itr.next();
            counter++;
        }
        return counter;
    }
}
