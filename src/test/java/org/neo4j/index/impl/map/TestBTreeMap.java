/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.index.impl.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.index.Neo4jTestCase;

public class TestBTreeMap extends Neo4jTestCase
{
	private BTreeMap<Character,Character> bTreeMap;
	
	@Before
	public void setUpMap()
	{
		Node bNode = graphDb().createNode();
		bTreeMap = new BTreeMap<Character,Character>( "test_map", bNode, graphDb() );
	}
	
	@After
	public void tearDownMap()
	{
 		bTreeMap.delete();
	}
	
	@Test
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
	}	
}
