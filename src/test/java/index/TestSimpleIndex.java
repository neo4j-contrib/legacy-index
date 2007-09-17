package index;

import java.util.Iterator;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.index.SimpleIndex;

public class TestSimpleIndex extends TestCase
{
	public TestSimpleIndex(String testName)
	{
		super( testName );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestSimpleIndex.class );
		return suite;
	}
	
	private SimpleIndex index;
	private EmbeddedNeo neo;
	Transaction tx;
	
	@Override
	public void setUp()
	{
		neo = new EmbeddedNeo( null, "var/timeline", true );
		tx = Transaction.begin();
		Node node = neo.createNode();
		index = new SimpleIndex( "test_simple", node, neo ); 
	}
	
	@Override
	public void tearDown()
	{
		index.drop();
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
		Object key2 = 2;
		index.index( node2, key2 );
		
		itr = index.getNodesFor( key1 ).iterator();
		assertEquals( node1, itr.next() );
		assertTrue( !itr.hasNext() );		
		itr = index.getNodesFor( key2 ).iterator();
		assertEquals( node2, itr.next() );
		assertTrue( !itr.hasNext() );		
		
		index.remove( node1, key1 );
		index.remove( node2, key2 );
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );
		assertTrue( !index.getNodesFor( key2 ).iterator().hasNext() );
		
		node1.delete();
		node2.delete();
		tx.success();
	}
	
	public void testIllegalStuff()
	{
		Node node1 = neo.createNode();
		Node node2 = neo.createNode();
		Object key1 = 1;
		Object key2 = 1;
		try 
		{ 
			new SimpleIndex( "blabla", null, neo );
			fail( "Null parameter should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		try 
		{ 
			new SimpleIndex( "blabla", node1, null );
			fail( "Null parameter should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}

		index.index( node1, key1 );
		try 
		{ 
			index.index( node1, key1 );
			fail( "Re-adding same index throw exception" );
		} 
		catch ( RuntimeException e ) { // good
		}
		try 
		{ 
			index.index( node2, key2 );
			fail( "Removing non existing index should throw exception" );
		} 
		catch ( RuntimeException e ) { // good
		}
		index.remove( node1, key1 );
		try 
		{ 
			index.remove( node1, key1 );
			fail( "Removing non existing index should throw exception" );
		} 
		catch ( RuntimeException e ) { // good
		}
		
		node1.delete();
		node2.delete();
		tx.success();
	}	
}
