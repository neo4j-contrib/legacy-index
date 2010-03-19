/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.index.impl.sortedtree;

import static org.junit.Assert.assertEquals;

import java.util.Comparator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.index.impl.btree.BTree.RelTypes;

public class TestSortedTree extends Neo4jTestCase
{
    private static final String VALUE = "value";
    
	private SortedTree bTree;
	
	@Before
	public void setUp() throws Exception
	{
		Node bNode = graphDb().createNode();
		graphDb().getReferenceNode().createRelationshipTo( bNode, 
			RelTypes.TREE_ROOT );
		bTree = new SortedTree( graphDb(), bNode, new NodeSorter() );
	}

	@After
	public void tearDown() throws Exception
	{
 		bTree.delete();
	}
    
	@Test
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
    
    private Node createNode( char c )
    {
        Node node = graphDb().createNode();
        node.setProperty( VALUE, c );
        return node;
    }

    static class NodeSorter implements Comparator<Node>
    {
        public int compare( Node o1, Node o2 )
        {
            Comparable c1 = (Comparable) o1.getProperty( VALUE );
            Comparable c2 = (Comparable) o2.getProperty( VALUE );
            return c1.compareTo( c2 );
        }
    }
}
