/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.index.Index;
import org.neo4j.index.IndexHits;
import org.neo4j.index.Neo4jTestCase;

public class TestSingleIndex extends Neo4jTestCase
{
	private SingleValueIndex index;
	
	@Before
	public void setUpIndex() throws Exception
	{
		Node node = graphDb().createNode();
		index = new SingleValueIndex( "test_simple", node, graphDb() ); 
	}
	
	@After
	public void tearDownIndex() throws Exception
	{
		index.drop();
	}
	
	@Test
	public void testSimpleIndexBasic()
	{
		Node node1 = graphDb().createNode();
		Object key1 = 1;
		
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		
		IndexHits hits = index.getNodesFor( key1 );
		Iterator<Node> itr = hits.iterator();
		assertEquals( node1, itr.next() );
		assertEquals( 1, hits.size() );
		assertTrue( !itr.hasNext() );
		
		index.remove( node1, key1 );
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		Node node2 = graphDb().createNode();
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
	}
	
	@Test
    public void testIllegalStuff()
	{
		Node node1 = graphDb().createNode();
		try 
		{ 
			new SingleValueIndex( "blabla", null, graphDb() );
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
		Index mIndex = new MultiValueIndex( "multi", node1, graphDb() );
		try 
		{ 
			new SingleValueIndex( "blabla", node1, graphDb() );
			fail( "Wrong index type should throw exception" );
		} 
		catch ( IllegalArgumentException e ) { // good
		}
		mIndex.drop();
	}
    
    public void testValues()
    {
        Set<Node> nodes = new HashSet<Node>();
        for ( int i = 0; i < 100; i++ )
        {
            Node node = graphDb().createNode();
            nodes.add( node );
            index.index( node, i );
        }
        for ( Node node : index.values() )
        {
            assertTrue( nodes.remove( node ) );
            node.delete();
        }
        assertTrue( nodes.isEmpty() );
    }
    
    public void testClear()
    {
        List<Node> nodes = new LinkedList<Node>();
        for ( int i = 0; i < 100; i++ )
        {
            Node node = graphDb().createNode();
            nodes.add( node );
            index.index( node, i );
        }
        index.clear();
        index.index( nodes.get( 0 ), 0 );
        index.remove( nodes.get( 0 ), 0 );
        for ( Node node : nodes )
        {
            node.delete();
        }
    }
}
