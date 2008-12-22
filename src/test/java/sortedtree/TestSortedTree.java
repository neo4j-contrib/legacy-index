package sortedtree;

import java.util.Comparator;

import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;
import org.neo4j.util.btree.BTree.RelTypes;
import org.neo4j.util.sortedtree.SortedTree;

public class TestSortedTree extends NeoTestCase
{
	private SortedTree bTree;
	
	@Override
	public void setUp() throws Exception
	{
	    super.setUp();
		Node bNode = neo().createNode();
		neo().getReferenceNode().createRelationshipTo( bNode, 
			RelTypes.TREE_ROOT );
		bTree = new SortedTree( neo(), bNode, new NodeSorter() );
	}

    private static final String VALUE = "value";
    
    static class NodeSorter implements Comparator<Node>
    {
        public int compare( Node o1, Node o2 )
        {
            Comparable c1 = (Comparable) o1.getProperty( VALUE );
            Comparable c2 = (Comparable) o2.getProperty( VALUE );
            return c1.compareTo( c2 );
        }
    }
    
	@Override
	public void tearDown() throws Exception
	{
 		bTree.delete();
 		super.tearDown();
	}
    
    public void testBasicSort()
    {
        bTree.addNode( createNode( 'c' ) );
        bTree.addNode( createNode( 'n' ) );
        bTree.addNode( createNode( 'g' ) );
        bTree.addNode( createNode( 'a' ) );
        bTree.addNode( createNode( 'h' ) );
        bTree.addNode( createNode( 'e' ) );
        bTree.addNode( createNode( 'k' ) );
        bTree.addNode( createNode( 'i' ) );

        bTree.addNode( createNode( 'q' ) );
        bTree.addNode( createNode( 'm' ) );
        bTree.addNode( createNode( 'f' ) );
        bTree.addNode( createNode( 'w' ) );
        bTree.addNode( createNode( 'l' ) );
        bTree.addNode( createNode( 't' ) );
        bTree.addNode( createNode( 'z' ) );
        
        bTree.addNode( createNode( 'd' ) );
        bTree.addNode( createNode( 'p' ) );
        bTree.addNode( createNode( 'r' ) );
        bTree.addNode( createNode( 'x' ) );
        bTree.addNode( createNode( 'y' ) );
        bTree.addNode( createNode( 's' ) );
        
        bTree.addNode( createNode( 'b' ) );
        bTree.addNode( createNode( 'j' ) );
        bTree.addNode( createNode( 'o' ) );
        bTree.addNode( createNode( 'u' ) );
        bTree.addNode( createNode( 'v' ) );

        char c = 'a';
        for ( Node node : bTree.getSortedNodes() )
        {
            assertEquals( c, node.getProperty( VALUE ) );
            c++;
        }
    }
    
    public Node createNode( char c )
    {
        Node node = neo().createNode();
        node.setProperty( VALUE, c );
        return node;
    }
}
