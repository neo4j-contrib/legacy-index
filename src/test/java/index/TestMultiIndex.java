package index;

import java.util.Iterator;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.index.MultiValueIndex;
import org.neo4j.util.index.SingleValueIndex;

public class TestMultiIndex extends TestCase
{
	public TestMultiIndex(String testName)
	{
		super( testName );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestMultiIndex.class );
		return suite;
	}
	
	private MultiValueIndex index;
	private EmbeddedNeo neo;
	Transaction tx;
	
	@Override
	public void setUp()
	{
		neo = new EmbeddedNeo( null, "var/timeline" );
		tx = Transaction.begin();
		Node node = neo.createNode();
		index = new MultiValueIndex( "test_simple", node, neo ); 
	}
	
	@Override
	public void tearDown()
	{
		// index.drop();
		tx.success();
		tx.finish();
		neo.shutdown();
	}
	
	public void testSimpleIndexBasic()
	{
		Node node1 = neo.createNode();
		Object key1 = 1;
		
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		
		Iterator<Node> itr = index.getNodesFor( key1 ).iterator();
		assertEquals( node1, itr.next() );
		assertTrue( !itr.hasNext() );
		
		index.remove( node1, key1 );
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		Node node2 = neo.createNode();
		index.index( node2, key1 );
		
		itr = index.getNodesFor( key1 ).iterator();
		assertTrue( itr.next() != null );
		assertTrue( itr.next() != null );
		assertTrue( !itr.hasNext() );
		assertTrue( !itr.hasNext() );		
		
		index.remove( node1, key1 );
		index.remove( node2, key1 );
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );
		
		node1.delete();
		node2.delete();
		tx.success();
	}
	
	public void testIllegalStuff()
	{
		Node node1 = neo.createNode();
		try 
		{ 
			new MultiValueIndex( "blabla", null, neo );
			fail( "Null parameter should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		try 
		{ 
			new MultiValueIndex( "blabla", node1, null );
			fail( "Null parameter should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		SingleValueIndex sIndex = new SingleValueIndex( "multi", node1, neo );
		try 
		{ 
			new MultiValueIndex( "blabla", node1, neo );
			fail( "Wrong index type should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		// mIndex.drop();
		tx.success();
	}	
}
