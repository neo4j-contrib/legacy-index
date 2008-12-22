package index;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;
import org.neo4j.util.index.Index;
import org.neo4j.util.index.MultiValueIndex;
import org.neo4j.util.index.SingleValueIndex;

public class TestMultiIndex extends NeoTestCase
{
	private MultiValueIndex index;
	
	@Override
	public void setUp() throws Exception
	{
	    super.setUp();
		Node node = neo().createNode();
		index = new MultiValueIndex( "test_simple", node, neo() ); 
	}
	
	public void testSimpleIndexBasic()
	{
		Node node1 = neo().createNode();
		Object key1 = 1;
		
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		
		Iterator<Node> itr = index.getNodesFor( key1 ).iterator();
		assertEquals( node1, itr.next() );
		assertTrue( !itr.hasNext() );
		
		index.remove( node1, key1 );
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		Node node2 = neo().createNode();
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
	}
	
	public void testIllegalStuff()
	{
		Node node1 = neo().createNode();
		try 
		{ 
			new MultiValueIndex( "blabla", null, neo() );
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
		Index sIndex = new SingleValueIndex( "multi", node1, neo() );
		try 
		{ 
			new MultiValueIndex( "blabla", node1, neo() );
			fail( "Wrong index type should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		sIndex.drop();
	}	

    public void testValues()
    {
        Set<Node> nodes = new HashSet<Node>();
        for ( int i = 0; i < 100; i++ )
        {
            Node node1 = neo().createNode();
            Node node2 = neo().createNode();
            nodes.add( node1 );
            nodes.add( node2 );
            index.index( node1, i );
            index.index( node2, i );
        }
        for ( Node node : index.values() )
        {
            assertTrue( nodes.remove( node ) );
        }
        assertTrue( nodes.isEmpty() );
    }
}
