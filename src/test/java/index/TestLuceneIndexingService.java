package index;

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
        indexService = new LuceneIndexService( neo );
		tx = neo.beginTx();
		Node node = neo.createNode();
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
}
