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
package org.neo4j.util.sortedtree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;

//not thread safe yet
public class SortedTree
{
	public static enum RelTypes implements RelationshipType
	{
		TREE_ROOT,
		SUB_TREE,
		// a relationship type where relationship actually is the *key entry*
		KEY_ENTRY 
	};
	
	private final NeoService neo;
    private final Comparator<Node> nodeComparator;
	private TreeNode treeRoot;
	
	public SortedTree( NeoService neo, Node rootNode, 
        Comparator<Node> nodeComparator )
	{
		this.neo = neo;
        this.nodeComparator = nodeComparator;
		this.treeRoot = new TreeNode( this, rootNode );
	}
	
	void makeRoot( TreeNode newRoot )
	{
		Relationship rel = treeRoot.getUnderlyingNode().getSingleRelationship( 
			RelTypes.TREE_ROOT, Direction.INCOMING );
		Node startNode = rel.getStartNode();
		rel.delete();
		startNode.createRelationshipTo( newRoot.getUnderlyingNode(), 
			RelTypes.TREE_ROOT );
		treeRoot = newRoot;
	}
	
	/**
	 * Deletes this sorted tree.
	 */
	public void delete()
	{
		Relationship rel = treeRoot.getUnderlyingNode().getSingleRelationship( 
			RelTypes.TREE_ROOT, Direction.INCOMING );
		treeRoot.delete();
		rel.delete();
	}
	
	/**
	 * Deletes this sorted tree using a commit interval.
	 * 
	 * @param commitInterval number of entries to remove before a new 
	 * transaction is started
	 */
	public void delete( int commitInterval )
	{
		Relationship rel = treeRoot.getUnderlyingNode().getSingleRelationship( 
			RelTypes.TREE_ROOT, Direction.INCOMING );
		treeRoot.delete( commitInterval, 0);
		rel.delete();
	}
	
	public boolean addNode( Node node )
	{
		return treeRoot.addEntry( node, true );
	}
    
    public boolean containsNode( Node node )
    {
        return treeRoot.containsEntry( node );
    }
	
	public boolean removeNode( Node node )
	{
		return treeRoot.removeEntry( node );
	}
	
	int getOrder()
	{
		return 9;
	}
	
	NeoService getNeo()
	{
		return neo;
	}
    
    public Comparator<Node> getComparator()
    {
        return nodeComparator;
    }
	
    public Iterable<Node> getSortedNodes()
    {
        List<Node> nodeList = new ArrayList<Node>();
        traverseTreeNode( treeRoot, nodeList );
        return nodeList;
    }
    
    private void traverseTreeNode( TreeNode currentNode, List<Node> nodeList )
    {
        NodeEntry entry = currentNode.getFirstEntry();
        while ( entry != null )
        {
            TreeNode beforeTree = entry.getBeforeSubTree();
            if ( beforeTree != null )
            {
                traverseTreeNode( beforeTree, nodeList );
            }
            nodeList.add( entry.getTheNode() );
            NodeEntry nextEntry = entry.getNextKey();
            if ( nextEntry == null )
            {
                TreeNode afterTree = entry.getAfterSubTree();
                if ( afterTree != null )
                {
                    traverseTreeNode( afterTree, nodeList );
                }
            }
            entry = nextEntry;
        }
    }
}