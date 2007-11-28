package index;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.index.Index;
import org.neo4j.util.index.MultiValueIndex;
import org.neo4j.util.index.SingleValueIndex;

public class TestSingleIndex extends TestCase
{
	public TestSingleIndex(String testName)
	{
		super( testName );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestSingleIndex.class );
		return suite;
	}
	
	private SingleValueIndex index;
	private NeoService neo;
	private Transaction tx;
	
	@Override
	public void setUp()
	{
		neo = new EmbeddedNeo( "var/index" );
		tx = neo.beginTx();
		Node node = neo.createNode();
		index = new SingleValueIndex( "test_simple", node, neo ); 
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
		try 
		{ 
			new SingleValueIndex( "blabla", null, neo );
			fail( "Null parameter should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		try 
		{ 
			new SingleValueIndex( "blabla", node1, null );
			fail( "Null parameter should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		Index mIndex = new MultiValueIndex( "multi", node1, neo );
		try 
		{ 
			new SingleValueIndex( "blabla", node1, neo );
			fail( "Wrong index type should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		mIndex.drop();
		tx.success();
	}
    
    public void testValues()
    {
        Set<Node> nodes = new HashSet<Node>();
        for ( int i = 0; i < 100; i++ )
        {
            Node node = neo.createNode();
            nodes.add( node );
            index.index( node, i );
        }
        for ( Node node : index.values() )
        {
            assertTrue( nodes.remove( node ) );
        }
        assertTrue( nodes.isEmpty() );
    }
}
