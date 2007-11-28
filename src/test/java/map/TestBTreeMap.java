package map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.map.BTreeMap;

public class TestBTreeMap extends TestCase
{
	public TestBTreeMap(String testName)
	{
		super( testName );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestBTreeMap.class );
		return suite;
	}
	
	private BTreeMap<Character,Character> bTreeMap;
	private EmbeddedNeo neo;
	private Transaction tx;
	
	@Override
	public void setUp()
	{
		neo = new EmbeddedNeo( "var/map" );
		tx = neo.beginTx();
		Node bNode = neo.createNode();
		bTreeMap = new BTreeMap<Character,Character>( "test_map", bNode, neo );
	}
	
	@Override
	public void tearDown()
	{
 		bTreeMap.delete();
		tx.finish();
		neo.shutdown();
	}
	
	public void testBasicBTreeMap()
	{
		assertNull( bTreeMap.put( 'c', 'a' ) );
		assertEquals( (Character) 'a', bTreeMap.put( 'c', 'c' ) );
		bTreeMap.put( 'n', 'n' );
		bTreeMap.put( 'g', 'g' );
		bTreeMap.put( 'a', 'a' );
		bTreeMap.put( 'h', 'h' );
		bTreeMap.put( 'e', 'e' );
		bTreeMap.put( 'k', 'k' );
		bTreeMap.put( 'q', 'q' );
		bTreeMap.put( 'm', 'm' );
		bTreeMap.put( 'f', 'f' );
		bTreeMap.put( 'w', 'w' );
		bTreeMap.put( 'l', 'l' );
		bTreeMap.put( 't', 't' );
		bTreeMap.put( 'z', 'z' );
		bTreeMap.put( 'd', 'd' );
		bTreeMap.put( 'p', 'p' );
		bTreeMap.put( 'r', 'r' );
		bTreeMap.put( 'x', 'x' );
		bTreeMap.put( 'y', 'y' );
		bTreeMap.put( 's', 's' );
		
		assertEquals( (Character) 'h', bTreeMap.remove( 'h' ) );
		assertNull( bTreeMap.remove( 'h' ) );
		assertEquals( (Character) 't', bTreeMap.remove( 't' ) );
		assertEquals( (Character) 'r', bTreeMap.remove( 'r' ) );
		tx.success();
	}	
}
