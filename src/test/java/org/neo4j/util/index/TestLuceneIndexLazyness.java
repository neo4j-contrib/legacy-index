package org.neo4j.util.index;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;

public class TestLuceneIndexLazyness extends NeoTestCase
{
    private IndexService indexService;
    
    protected IndexService instantiateIndexService()
    {
        return new LuceneIndexService( neo() );
    }
    
    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        indexService = instantiateIndexService();
    }
    
    protected IndexService indexService()
    {
        return indexService;
    }
    
    @Override
    protected void beforeNeoShutdown()
    {
        indexService().shutdown();
    }
    
    public void testIt() throws Exception
    {
        String key = "mykey";
        String value = "myvalue";
        Collection<Node> nodes = new ArrayList<Node>();
        for ( int i = 0; i < 5000; i++ )
        {
            Node node = neo().createNode();
            indexService.index( node, key, value );
            nodes.add( node );
            if ( i == 2000 )
            {
                Iterable<Node> itr = indexService.getNodes( key, value );
                assertCollection( asCollection( itr ),
                    nodes.toArray( new Node[ 0 ] ) );
            }
            
            if ( i % 1000 == 0 )
            {
                restartTx();
            }
        }
        restartTx();
        
        Node[] nodeArray = nodes.toArray( new Node[ 0 ] );
        long total = 0;
        long totalTotal = 0;
        int counter = 0;
        for ( int i = 0; i < 10; i++ )
        {
            // So that it'll get the nodes in the synchronous way
            ( ( LuceneIndexService ) indexService ).setLazynessThreshold(
                nodes.size() + 10 );
            long time = System.currentTimeMillis();
            Iterable<Node> itr = indexService.getNodes( key, value );
            long syncTime = System.currentTimeMillis() - time;
            assertCollection( asCollection( itr ), nodeArray );
            long syncTotalTime = System.currentTimeMillis() - time;
            
            // So that it'll get the nodes in the lazy way
            ( ( LuceneIndexService ) indexService ).setLazynessThreshold(
                nodes.size() - 10 );
            time = System.currentTimeMillis();
            itr = indexService.getNodes( key, value );
            long lazyTime = System.currentTimeMillis() - time;
            assertCollection( asCollection( itr ), nodeArray );
            long lazyTotalTime = System.currentTimeMillis() - time;
            System.out.println( "lazy:" + lazyTime + " (" + lazyTotalTime +
                "), sync:" + syncTime + " (" + syncTotalTime + ")" );
            
            if ( i > 0 )
            {
                total += syncTime;
                totalTotal += syncTotalTime;
                counter++;
            }
            assertTrue( lazyTime < syncTime / 2 );
        }
        
        System.out.println( "avg:" + ( total / counter ) + ", " +
            ( totalTotal / counter ) );
        
        for ( Node node : nodes )
        {
            node.delete();
        }
    }
}
