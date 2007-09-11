package org.neo4j.util.btree;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.util.btree.BTree.RelTypes;

public class KeyEntry
{
	static final String KEY = "key";
	static final String VALUE = "val";
	
	private Relationship entryRelationship;
	private TreeNode treeNode;
	
	KeyEntry( TreeNode treeNode, Relationship underlyingRelationship )
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
	
	private BTree getBTree()
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
	
	KeyEntry getNextKey()
	{
		Relationship nextKeyRel = getEndNode().getSingleRelationship( 
			RelTypes.KEY_ENTRY, Direction.OUTGOING );
		if ( nextKeyRel != null )
		{
			return new KeyEntry( getTreeNode(), nextKeyRel );
		}
		return null;
	}
	
	KeyEntry getPreviousKey()
	{
		Relationship prevKeyRel = getStartNode().getSingleRelationship( 
			RelTypes.KEY_ENTRY, Direction.INCOMING );
		if ( prevKeyRel != null )
		{
			return new KeyEntry( getTreeNode(), prevKeyRel );
		}
		return null;
	}
	
	public long getKey()
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
	
	public String toString()
	{
		return "Entry[" + getKey() + "," + getValue() + "]";
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
		long key = getKey();
		Object value = getValue();
		entryRelationship.delete();
		entryRelationship = startNode.createRelationshipTo( endNode, 
			RelTypes.KEY_ENTRY );
		setKey( key );
		setValue( value );
    }
}
