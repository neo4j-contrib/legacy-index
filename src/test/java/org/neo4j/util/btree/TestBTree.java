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
package org.neo4j.util.btree;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;
import org.neo4j.util.btree.BTree;
import org.neo4j.util.btree.BTree.RelTypes;

public class TestBTree extends NeoTestCase
{
	private BTree bTree;
	
	@Override
	public void setUp() throws Exception
	{
	    super.setUp();
		Node bNode = neo().createNode();
		neo().getReferenceNode().createRelationshipTo( bNode, 
			RelTypes.TREE_ROOT );
		bTree = new BTree( neo(), bNode );
	}
	
	@Override
	protected void tearDown() throws Exception
	{
	    bTree.delete();
	    super.tearDown();
	}
	
	public void testBasicBTree()
	{
		bTree.addEntry( 'c', 'c' );
		bTree.addEntry( 'n', 'n' );
		bTree.addEntry( 'g', 'g' );
		bTree.addEntry( 'a', 'a' );
		bTree.addEntry( 'h', 'h' );
		bTree.addEntry( 'e', 'e' );
		bTree.addEntry( 'k', 'k' );
		bTree.addEntry( 'q', 'q' );
		bTree.addEntry( 'm', 'm' );
		bTree.addEntry( 'f', 'f' );
		bTree.addEntry( 'w', 'w' );
		bTree.addEntry( 'l', 'l' );
		bTree.addEntry( 't', 't' );
		bTree.addEntry( 'z', 'z' );
		bTree.addEntry( 'd', 'd' );
		bTree.addEntry( 'p', 'p' );
		bTree.addEntry( 'r', 'r' );
		bTree.addEntry( 'x', 'x' );
		bTree.addEntry( 'y', 'y' );
		bTree.addEntry( 's', 's' );
		
		bTree.validateTree();
		assert bTree.removeEntry( 'h' ).equals( 'h' );
		assert bTree.removeEntry( 't' ).equals( 't' );
		assert bTree.removeEntry( 'r' ).equals( 'r' );
		bTree.validateTree();
		assert bTree.removeEntry( 'e' ).equals( 'e' );
		assert bTree.removeEntry( 'a' ).equals( 'a' );
		assert bTree.removeEntry( 'x' ).equals( 'x' );
		assert bTree.removeEntry( 'y' ).equals( 'y' );
		assert bTree.removeEntry( 'z' ).equals( 'z' );
		assert bTree.removeEntry( 'w' ).equals( 'w' );
		bTree.validateTree();
		assert bTree.removeEntry( 's' ).equals( 's' );
		assert bTree.removeEntry( 'q' ).equals( 'q' );
		assert bTree.removeEntry( 'm' ).equals( 'm' );
		assert bTree.removeEntry( 'n' ).equals( 'n' );
		assert bTree.removeEntry( 'p' ).equals( 'p' );
		assert bTree.removeEntry( 'k' ).equals( 'k' );
		bTree.validateTree();
		assert bTree.removeEntry( 'l' ).equals( 'l' );
		assert bTree.removeEntry( 'g' ).equals( 'g' );
		assert bTree.removeEntry( 'c' ).equals( 'c' );
		assert bTree.removeEntry( 'd' ).equals( 'd' );
		assert bTree.removeEntry( 'f' ).equals( 'f' );
		bTree.validateTree();
	}
	
	private long getNextUniqueLong( Set<Long> usedValues, java.util.Random r )
	{
		long value = r.nextLong();
		while ( usedValues.contains( value ) )
		{
			value = r.nextInt();
		}
		usedValues.add( value );
		return value;
	}
	
	public void testSomeMore()
	{
		java.util.Random r = new java.util.Random( System.currentTimeMillis() );
		long[] values = new long[500];
		Set<Long> valueSet = new HashSet<Long>();
		for ( int i = 0; i < values.length; i++ )
		{
			values[i] = getNextUniqueLong( valueSet, r );
		}
		for ( int i = 0; i < values.length; i++ )
		{
			assertTrue( bTree.getEntry( values[i] ) == null );
			bTree.addEntry( values[i], values[i] );
		}
		bTree.validateTree();
		for ( int i = values.length -1; i > -1; i-- )
		{
			assertEquals( bTree.getEntry( values[i] ), values[i] );
		}
 		Collections.shuffle( Arrays.asList( values ) );
		for ( int i = 0; i < values.length; i++ )
		{
			assertEquals( bTree.removeEntry( values[i] ), values[i] );
			assertTrue( bTree.getEntry( values[i] ) == null );
			if ( i % 100 == 0 )
			{
				bTree.validateTree();
			}
		}
	}
	
	public void testGetValues()
	{
		bTree.addEntry( 'c', 'c' );
		bTree.addEntry( 'n', 'n' );
		bTree.addEntry( 'g', 'g' );
		bTree.addEntry( 'a', 'a' );
		bTree.addEntry( 'h', 'h' );
		bTree.addEntry( 'e', 'e' );

		for ( Object value : bTree.values() )
		{
			assertTrue( value instanceof Character );
		}
	}
	
	public void testClosestEntry()
	{
		for ( long i = 1; i < 256; i+=2 )
		{
			bTree.addEntry( i, i );
		}
		for ( long i = 0; i < 255; i+=2 )
		{
			assertEquals( i + 1, bTree.getClosestHigherEntry( i ) );
		}
		for ( long i = 2; i < 257; i+=2 )
		{
			assertEquals( i - 1, bTree.getClosestLowerEntry( i ) );
		}
	}
}
