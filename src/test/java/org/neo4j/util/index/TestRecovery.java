package org.neo4j.util.index;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.TestCase;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.transaction.XidImpl;

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
    
    public void testReCommit()
    {
        String path = "var/recovery";
        deleteFileOrDirectory( new File( path ) );
        NeoService neo = new EmbeddedNeo( path );
        IndexService idx = new LuceneIndexService( neo );
        Transaction tx = neo.beginTx();
        assertEquals( null, idx.getSingleNode( "test", "1" ) );
        Node refNode = neo.getReferenceNode();
        tx.finish();
        idx.shutdown();
        Map<String,String> params = new HashMap<String,String>();
        params.put( "dir", path + "/lucene" );
        try
        {
            LuceneDataSource xaDs = new LuceneDataSource( params );
            LuceneXaConnection xaC = (LuceneXaConnection) xaDs.getXaConnection();
            XAResource xaR = xaC.getXaResource();
            Xid xid = new XidImpl( new byte[1], new byte[1] );
            xaR.start( xid, XAResource.TMNOFLAGS );
            xaC.index( refNode, "test", "1" );
            xaR.end( xid, XAResource.TMSUCCESS );
            xaR.prepare( xid );
            xaR.commit( xid, false );
            copyLogicalLog( "var/recovery/lucene/lucene.log.active", 
                "var/recovery/lucene/lucene.log.active.bak" );
            copyLogicalLog( "var/recovery/lucene/lucene.log.1", 
                "var/recovery/lucene/lucene.log.1.bak" );
            // test recovery re-commit
            idx = new LuceneIndexService( neo );
            tx = neo.beginTx();
            assertEquals( refNode, idx.getSingleNode( "test", "1" ) );
            tx.finish();
            idx.shutdown();
            assertTrue( 
                new File( "var/recovery/lucene/lucene.log.active" ).delete() );
            // test recovery again on same log and only still only get 1 node
            copyLogicalLog( "var/recovery/lucene/lucene.log.active.bak", 
                "var/recovery/lucene/lucene.log.active" );
            copyLogicalLog( "var/recovery/lucene/lucene.log.1.bak", 
                "var/recovery/lucene/lucene.log.1" );
            idx = new LuceneIndexService( neo );
            tx = neo.beginTx();
            assertEquals( refNode, idx.getSingleNode( "test", "1" ) );
            tx.finish();
            idx.shutdown();
            neo.shutdown();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
        
    }

    private void copyLogicalLog( String name, String copy ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        assertTrue( new File( name ).exists() );
        FileChannel source = new RandomAccessFile( name, "r" ).getChannel();
        assertTrue( !new File( copy ).exists() );
        FileChannel dest = new RandomAccessFile( copy, "rw" ).getChannel();
        int read = -1;
        do
        {
            read = source.read( buffer );
            buffer.flip();
            dest.write( buffer );
            buffer.clear();
        }
        while ( read == 1024 );
        source.close();
        dest.close();
    }
}
