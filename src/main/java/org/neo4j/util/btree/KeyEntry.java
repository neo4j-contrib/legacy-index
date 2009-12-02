/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.util.btree;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.util.btree.BTree.RelTypes;
import org.neo4j.util.map.BTreeMap;

/**
 * Wraps the functionality of one entry in the tree.
 */
public class KeyEntry
{
	static final String KEY = "key";
	static final String VALUE = "val";
	static final String KEY_VALUE = "key_val";
	
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
	
	/**
	 * @return the key for this entry. This is the key added via
	 * {@link BTree#addEntry(long, Object)}.
	 */
	public long getKey()
	{
		return (Long) entryRelationship.getProperty( KEY );
	}
	
	void setKey( long key )
	{
		entryRelationship.setProperty( KEY, key );
	}
	
	/**
	 * @return the value for this entry. This is the value added via
	 * {@link BTree#addEntry(long, Object)}.
	 */
	public Object getValue()
	{
		return entryRelationship.getProperty( VALUE );
	}
	
	/**
	 * Sets or changes the value for this entry.
	 * 
	 * @param value the new value for this entry. The type of the value must
	 * be one of types supported by neo4j.
	 */
	public void setValue( Object value )
	{
		entryRelationship.setProperty( VALUE, value );
	}
	
	/**
	 * This is optional and is used in some implementations, f.ex
	 * {@link BTreeMap}.
	 * 
	 * @param keyValue represents the actual key which we can derive the
     * {@link #getKey()} from, f.ex. a String.
	 */
	public void setKeyValue( Object keyValue )
	{
		entryRelationship.setProperty( KEY_VALUE, keyValue );
	}
	
	/**
	 * @return the actual key which we can derive the {@link #getKey()} from,
	 * f.ex. a String.
	 */
	public Object getKeyValue()
	{
		return entryRelationship.getProperty( KEY_VALUE, null );
	}
	
	/**
	 * Removes this entry from the b-tree.
	 */
	public void remove()
	{
		treeNode.removeEntry( this.getKey() );
	}
	
	@Override
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
		Object keyValue = getKeyValue();
		entryRelationship.delete();
		entryRelationship = startNode.createRelationshipTo( endNode, 
			RelTypes.KEY_ENTRY );
		setKey( key );
		setValue( value );
		if ( keyValue != null )
		{
			setKeyValue( keyValue );
		}
    }
}
