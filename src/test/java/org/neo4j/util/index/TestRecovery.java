package org.neo4j.util.index;

import java.io.File;
import java.util.Random;

import junit.framework.TestCase;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;

public class TestRecovery extends TestCase
{
    private void deleteFileOrDirectory( File file )
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
    
    public void testRecovery() throws Exception
    {
        String path = "var/recovery";
        deleteFileOrDirectory( new File( path ) );
        final NeoService neo = new EmbeddedNeo( path );
        final IndexService index = new LuceneIndexService( neo );
        
        neo.beginTx();
        Node node = neo.createNode();
        Random random = new Random();
        Thread stopper = new Thread()
        {
            @Override public void run()
            {
                sleepNice( 1000 );
                index.shutdown();
                neo.shutdown();
            }
        };
        try
        {
            stopper.start();
            for ( int i = 0; i < 500; i++ )
            {
                index.index( node, "" + random.nextInt(), random.nextInt() );
                sleepNice( 10 );
            }
        }
        catch ( Exception e )
        {
            // Ok
        }
        
        sleepNice( 1000 );
        final NeoService newNeo = new EmbeddedNeo( path );
        final IndexService newIndexService = new LuceneIndexService( newNeo );
        sleepNice( 1000 );
        newIndexService.shutdown();
        newNeo.shutdown();
    }
    
    private static void sleepNice( long time )
    {
        try
        {
            Thread.sleep( time );
        }
        catch ( InterruptedException e )
        {
            // Ok
        }
    }
}
