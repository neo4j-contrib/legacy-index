package org.neo4j.util.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.Isolation;
import org.neo4j.util.index.LuceneIndexService;

public class TestLuceneIndexingService extends NeoTestCase
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
	
    public void testSimple()
    {
        Node node1 = neo().createNode();
        
        assertTrue( !indexService().getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService().index( node1, "a_property", 1 );
        
        Iterator<Node> itr = indexService().getNodes( "a_property", 
            1 ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        
        indexService().removeIndex( node1, "a_property", 1 );
        assertTrue( !indexService().getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService().index( node1, "a_property", 1 );
        Node node2 = neo().createNode();
        indexService().index( node2, "a_property", 1 );
        
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( itr.next() != null );
        assertTrue( itr.next() != null );
        assertTrue( !itr.hasNext() );
        assertTrue( !itr.hasNext() );       
        
        indexService().removeIndex( node1, "a_property", 1 );
        indexService().removeIndex( node2, "a_property", 1 );
        assertTrue( !indexService().getNodes( "a_property", 
            1 ).iterator().hasNext() );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        restartTx();
        
        indexService().setIsolation( Isolation.ASYNC_OTHER_TX );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        
        assertTrue( !itr.hasNext() );
        indexService().index( node1, "a_property", 1 );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( itr.hasNext() );
        indexService().setIsolation( Isolation.SYNC_OTHER_TX );
        indexService().removeIndex( node1, "a_property", 1 );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        node1.delete();
        node2.delete();
    }
    
    public void testMultipleAdd()
    {
        Node node = neo().createNode();
        indexService().index( node, "a_property", 3 );
        restartTx();
        indexService().index( node, "a_property", 3 );
        restartTx();
        indexService().removeIndex( node, "a_property", 3 );
        restartTx();
        assertTrue( indexService().getSingleNode( "a_property", 3 ) == null );
    }
    
    public void testCaching()
    {
        String key = "prop";
        Object value = 10;
        ( ( LuceneIndexService ) indexService() ).enableCache( key, 1000 );
        Node node1 = neo().createNode();
        indexService().index( node1, key, value );
        indexService().getNodes( key, value );
        restartTx( false );
        
        Node node2 = neo().createNode();
        indexService().getNodes( key, value );
        indexService().index( node2, key, value );
        indexService().getNodes( key, value );
        restartTx();
        
        Node node3 = neo().createNode();
        indexService().getNodes( key, value );
        indexService().index( node3, key, value );
        indexService().getNodes( key, value );
        restartTx( false );
        
        Node node4 = neo().createNode();
        indexService().getNodes( key, value );
        indexService().index( node4, key, value );
        indexService().getNodes( key, value );
        restartTx();
        
        assertCollection( indexService().getNodes( key, value ),
            Arrays.asList( node2, node4 ) );
        
        indexService().removeIndex( node2, key, value );
        indexService().removeIndex( node4, key, value );
        node2.delete();
        node4.delete();
    }
    
    public void testRollback()
    {
        Node node1 = neo().createNode();
        Node node2 = neo().createNode();
        restartTx();
        indexService().index( node1, "a_property", 3 );
        assertEquals( node1, indexService().getSingleNode( "a_property", 3 ) );
        restartTx( false );
        assertEquals( null, indexService().getSingleNode( "a_property", 3 ) );
        indexService().index( node2, "a_property", 3 );
        assertEquals( node2, indexService().getSingleNode( "a_property", 3 ) );
        restartTx();
        assertEquals( node2, indexService().getSingleNode( "a_property", 3 ) );
    }
    
//    public void testDifferentTypesWithSameValueIssue()
//    {
//        String key = "prop";
//        Integer valueAsInt = 10;
//        String valueAsString = "10";
//        
//        Node node1 = neo().createNode();
//        indexService.index( node1, key, valueAsInt );
//        Node node2 = neo().createNode();
//        indexService.index( node2, key, valueAsString );
//        
//        assertCollection( indexService.getNodes( key, valueAsInt ), node1 );
//        assertCollection( indexService.getNodes( key, valueAsString ), node2 );
//        
//        tx.success(); tx.finish(); tx = neo().beginTx();
//        
//        assertCollection( indexService.getNodes( key, valueAsInt ), node1 );
//        assertCollection( indexService.getNodes( key, valueAsString ), node2 );
//        
//        indexService.removeIndex( node1, key, valueAsInt );
//        indexService.removeIndex( node2, key, valueAsString );
//        
//        node2.delete();
//        node1.delete();
//    }
    
    private <T> void assertCollection( Iterable<T> items,
        Iterable<T> expectedItems )
    {
        Collection<T> set = new HashSet<T>();
        for ( T item : items )
        {
            set.add( item );
        }
        
        int counter = 0;
        for ( T expectedItem : expectedItems )
        {
            assertTrue( set.contains( expectedItem ) );
            counter++;
        }
        assertEquals( counter, set.size() );
    }
    
//    private <T> void assertCollection( Iterable<T> items, T... expectedItems )
//    {
//        assertCollection( items, Arrays.asList( expectedItems ) );
//    }
}
