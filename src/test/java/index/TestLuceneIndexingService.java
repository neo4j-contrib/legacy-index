package index;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.Isolation;
import org.neo4j.util.index.LuceneIndexService;

public class TestLuceneIndexingService extends TestCase
{
	public TestLuceneIndexingService(String testName)
	{
		super( testName );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestLuceneIndexingService.class );
		return suite;
	}
	
	private IndexService indexService;
	private NeoService neo;
	private Transaction tx;
	
	@Override
	public void setUp()
	{
		neo = new EmbeddedNeo( "var/index" );
        indexService = new MyTestLuceneIndexService( neo );
		tx = neo.beginTx();
	}
	
	@Override
	public void tearDown()
	{
		// index.drop();
		tx.success();
		tx.finish();
        indexService.shutdown();
		neo.shutdown();
	}
    
    public void testSimple()
    {
        Node node1 = neo.createNode();
        
        assertTrue( !indexService.getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService.index( node1, "a_property", 1 );
        
        Iterator<Node> itr = indexService.getNodes( "a_property", 
            1 ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        
        indexService.removeIndex( node1, "a_property", 1 );
        assertTrue( !indexService.getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService.index( node1, "a_property", 1 );
        Node node2 = neo.createNode();
        indexService.index( node2, "a_property", 1 );
        
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( itr.next() != null );
        assertTrue( itr.next() != null );
        assertTrue( !itr.hasNext() );
        assertTrue( !itr.hasNext() );       
        
        indexService.removeIndex( node1, "a_property", 1 );
        indexService.removeIndex( node2, "a_property", 1 );
        assertTrue( !indexService.getNodes( "a_property", 
            1 ).iterator().hasNext() );
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        tx.success();
        tx.finish();
        tx = neo.beginTx();
        
        indexService.setIsolation( Isolation.ASYNC_OTHER_TX );
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        
        assertTrue( !itr.hasNext() );
        indexService.index( node1, "a_property", 1 );
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( itr.hasNext() );
        indexService.setIsolation( Isolation.SYNC_OTHER_TX );
        indexService.removeIndex( node1, "a_property", 1 );
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        node1.delete();
        node2.delete();
        tx.success();
    }
    
    public void testMultipleAdd()
    {
        Node node = neo.createNode();
        indexService.index( node, "a_property", 3 );
        tx.success(); tx.finish();
        tx = neo.beginTx();
        indexService.index( node, "a_property", 3 );
        tx.success(); tx.finish();
        tx = neo.beginTx();
        indexService.removeIndex( node, "a_property", 3 );
        tx.success(); tx.finish();
        tx = neo.beginTx();
        assertTrue( indexService.getSingleNode( "a_property", 3 ) == null );
    }
    
    public void testCaching()
    {
        String key = "prop";
        Object value = 10;
        ( ( MyTestLuceneIndexService ) indexService ).doEnableCache( key );
        Node node1 = neo.createNode();
        indexService.index( node1, key, value );
        indexService.getNodes( key, value );
        tx.failure(); tx.finish(); tx = neo.beginTx();
        
        Node node2 = neo.createNode();
        indexService.getNodes( key, value );
        indexService.index( node2, key, value );
        indexService.getNodes( key, value );
        tx.success(); tx.finish(); tx = neo.beginTx();
        
        Node node3 = neo.createNode();
        indexService.getNodes( key, value );
        indexService.index( node3, key, value );
        indexService.getNodes( key, value );
        tx.failure(); tx.finish(); tx = neo.beginTx();
        
        Node node4 = neo.createNode();
        indexService.getNodes( key, value );
        indexService.index( node4, key, value );
        indexService.getNodes( key, value );
        tx.success(); tx.finish(); tx = neo.beginTx();
        
        assertCollection( indexService.getNodes( key, value ),
            Arrays.asList( node2, node4 ) );
        
        indexService.removeIndex( node2, key, value );
        indexService.removeIndex( node4, key, value );
        node2.delete();
        node4.delete();
    }
    
//    public void testDifferentTypesWithSameValueIssue()
//    {
//        String key = "prop";
//        Integer valueAsInt = 10;
//        String valueAsString = "10";
//        
//        Node node1 = neo.createNode();
//        indexService.index( node1, key, valueAsInt );
//        Node node2 = neo.createNode();
//        indexService.index( node2, key, valueAsString );
//        
//        assertCollection( indexService.getNodes( key, valueAsInt ), node1 );
//        assertCollection( indexService.getNodes( key, valueAsString ), node2 );
//        
//        tx.success(); tx.finish(); tx = neo.beginTx();
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
    
    private <T> void assertCollection( Iterable<T> items, T... expectedItems )
    {
        assertCollection( items, Arrays.asList( expectedItems ) );
    }
    
    private static class MyTestLuceneIndexService extends LuceneIndexService
    {
        public MyTestLuceneIndexService( NeoService neo )
        {
            super( neo );
        }
        
        public void doEnableCache( String cache )
        {
            enableCache( cache, 1000 );
        }
    }
}
