package org.neo4j.util.index;

import java.util.Iterator;

import org.neo4j.api.core.Node;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.Isolation;
import org.neo4j.util.index.LuceneFulltextIndexService;

public class TestLuceneFulltextIndexService extends TestLuceneIndexingService
{
    @Override
    protected IndexService instantiateIndexService()
    {
        return new LuceneFulltextIndexService( neo() );
    }
    
    @Override
    public void testCaching()
    {
        // Do nothing
    }

    public void testSimpleFulltext()
    {
        Node node1 = neo().createNode();
        
        String partOfValue1 = "tokenize";
        String value1 = "A value with spaces in it which the fulltext " +
            "index should " + partOfValue1;
        String value2 = "Another value with spaces in it";
        String key = "some_property";
        assertTrue( !indexService().getNodes( key, 
            value1 ).iterator().hasNext() );
        assertTrue( !indexService().getNodes( key, 
            value2 ).iterator().hasNext() );

        indexService().index( node1, key, value1 );
        
        Iterator<Node> itr = indexService().getNodes( key, 
            partOfValue1 ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        
        indexService().removeIndex( node1, key, value1 );
        assertTrue( !indexService().getNodes( key, 
            value1 ).iterator().hasNext() );

        indexService().index( node1, key, value1 );
        Node node2 = neo().createNode();
        indexService().index( node2, key, value1 );
        
        itr = indexService().getNodes( key, partOfValue1 ).iterator();
        assertTrue( itr.next() != null );
        assertTrue( itr.next() != null );
        assertTrue( !itr.hasNext() );
        assertTrue( !itr.hasNext() );       
        
        indexService().removeIndex( node1, key, value1 );
        indexService().removeIndex( node2, key, value1 );
        assertTrue( !indexService().getNodes( key, 
            value1 ).iterator().hasNext() );
        itr = indexService().getNodes( key, value1 ).iterator();
        assertTrue( !itr.hasNext() );
        restartTx();
        
        indexService().setIsolation( Isolation.ASYNC_OTHER_TX );
        itr = indexService().getNodes( key, value1 ).iterator();
        
        assertTrue( !itr.hasNext() );
        indexService().index( node1, key, value1 );
        itr = indexService().getNodes( key, partOfValue1 ).iterator();
        assertTrue( !itr.hasNext() );
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        itr = indexService().getNodes( key, partOfValue1 ).iterator();
        assertTrue( itr.hasNext() );
        indexService().setIsolation( Isolation.SYNC_OTHER_TX );
        indexService().removeIndex( node1, key, value1 );
        itr = indexService().getNodes( key, value1 ).iterator();
        assertTrue( !itr.hasNext() );
        node1.delete();
        node2.delete();
    }
}
