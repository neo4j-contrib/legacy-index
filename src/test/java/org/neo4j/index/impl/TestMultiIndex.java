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
package org.neo4j.index.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.index.Index;
import org.neo4j.index.IndexHits;
import org.neo4j.index.NeoTestCase;

public class TestMultiIndex extends NeoTestCase
{
	private MultiValueIndex index;
	
	@Override
	public void setUp() throws Exception
	{
	    super.setUp();
		Node node = graphDb().createNode();
		index = new MultiValueIndex( "test_simple", node, graphDb() ); 
	}
	
	public void testSimpleIndexBasic()
	{
		Node node1 = graphDb().createNode();
		Object key1 = 1;
		
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		
		IndexHits hits = index.getNodesFor( key1 );
		Iterator<Node> itr = hits.iterator();
		assertEquals( 1, hits.size() );
		assertEquals( node1, itr.next() );
		assertTrue( !itr.hasNext() );
		
		index.remove( node1, key1 );
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );

		index.index( node1, key1 );
		Node node2 = graphDb().createNode();
		index.index( node2, key1 );
		
		hits = index.getNodesFor( key1 );
		itr = hits.iterator();
		assertTrue( itr.next() != null );
		assertTrue( itr.next() != null );
		assertTrue( !itr.hasNext() );
		assertTrue( !itr.hasNext() );		
		assertEquals( 2, hits.size() );
		
		index.remove( node1, key1 );
		index.remove( node2, key1 );
		assertTrue( !index.getNodesFor( key1 ).iterator().hasNext() );
		
		node1.delete();
		node2.delete();
	}
	
	public void testIllegalStuff()
	{
		Node node1 = graphDb().createNode();
		try 
		{ 
			new MultiValueIndex( "blabla", null, graphDb() );
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
		Index sIndex = new SingleValueIndex( "multi", node1, graphDb() );
		try 
		{ 
			new MultiValueIndex( "blabla", node1, graphDb() );
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
            Node node1 = graphDb().createNode();
            Node node2 = graphDb().createNode();
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
