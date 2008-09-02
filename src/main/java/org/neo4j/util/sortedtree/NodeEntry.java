package org.neo4j.util.sortedtree;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.util.btree.BTree.RelTypes;

public class NodeEntry
{
	static final String NODE_ID = "node_id";
	
	private Relationship entryRelationship;
	private TreeNode treeNode;
	
	NodeEntry( TreeNode treeNode, Relationship underlyingRelationship )
	{
		assert treeNode != null;
		assert underlyingRelationship != null;
		this.treeNode = treeNode;
		this.entryRelationship = underlyingRelationship;
	}
	
	Relationship getUnderlyingRelationship()
	{
		return entryRelationship;
	}
	
	TreeNode getTreeNode()
	{
		return treeNode;
	}
	
	private SortedTree getBTree()
	{
		return treeNode.getBTree();
	}
	
	TreeNode getBeforeSubTree()
	{
		Relationship subTreeRel = getStartNode().getSingleRelationship( 
			RelTypes.SUB_TREE, Direction.OUTGOING );
		if ( subTreeRel != null )
		{
			return new TreeNode( getBTree(), 
				subTreeRel.getEndNode() );
		}
		return null;
	}
	
	TreeNode getAfterSubTree()
	{
		Relationship subTreeRel = getEndNode().getSingleRelationship( 
			RelTypes.SUB_TREE, Direction.OUTGOING );
		if ( subTreeRel != null )
		{
			return new TreeNode( getBTree(), 
				subTreeRel.getEndNode() );
		}
		return null;
	}
	
	NodeEntry getNextKey()
	{
		Relationship nextKeyRel = getEndNode().getSingleRelationship( 
			RelTypes.KEY_ENTRY, Direction.OUTGOING );
		if ( nextKeyRel != null )
		{
			return new NodeEntry( getTreeNode(), nextKeyRel );
		}
		return null;
	}
	
	NodeEntry getPreviousKey()
	{
		Relationship prevKeyRel = getStartNode().getSingleRelationship( 
			RelTypes.KEY_ENTRY, Direction.INCOMING );
		if ( prevKeyRel != null )
		{
			return new NodeEntry( getTreeNode(), prevKeyRel );
		}
		return null;
	}
	
/*	public long getKey()
	{
		return (Long) entryRelationship.getProperty( KEY );
	}
	
	void setKey( long key )
	{
		entryRelationship.setProperty( KEY, key );
	}
	
	public Object getValue()
	{
		return entryRelationship.getProperty( VALUE );
	}
	
	public void setValue( Object value )
	{
		entryRelationship.setProperty( VALUE, value );
	}
	
	public void setKeyValue( Object keyValue )
	{
		entryRelationship.setProperty( KEY_VALUE, keyValue );
	}
	
	public Object getKeyValue()
	{
		return entryRelationship.getProperty( KEY_VALUE, null );
	}*/
	
	public void remove()
	{
        treeNode.removeEntry( this.getTheNode() );
	}
    
	@Override
	public String toString()
	{
		return "Entry[" + getTheNode() + "]";
	}
	
	boolean isLeaf()
	{
		if ( getUnderlyingRelationship().getStartNode().getSingleRelationship( 
			RelTypes.SUB_TREE, Direction.OUTGOING ) != null )
		{
			assert getUnderlyingRelationship().getEndNode().
				getSingleRelationship( RelTypes.SUB_TREE, Direction.OUTGOING )
				!= null;
			return false;
		}
		assert getUnderlyingRelationship().getEndNode().getSingleRelationship( 
			RelTypes.SUB_TREE, Direction.OUTGOING ) == null;
		return true;
	}
	
    Node getTheNode()
    {
        return getBTree().getNeo().getNodeById( 
            (Long) getUnderlyingRelationship().getProperty( NODE_ID ) ); 
    }
    
    void setTheNode( Node node )
    {
        getUnderlyingRelationship().setProperty( NODE_ID, node.getId() );
    }
    
    
	Node getStartNode()
	{
		return entryRelationship.getStartNode();
	}
	
	Node getEndNode()
	{
		return entryRelationship.getEndNode();
	}

	void move( TreeNode node, Node startNode, Node endNode )
    {
		assert node != null;
		this.treeNode = node;
		Node theNode = getTheNode();
		entryRelationship.delete();
		entryRelationship = startNode.createRelationshipTo( endNode, 
			RelTypes.KEY_ENTRY );
        setTheNode( theNode );
    }
}
