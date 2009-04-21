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
package org.neo4j.util.map;

import junit.framework.TestCase;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.map.BTreeMap;

public class TestBTreeMap extends TestCase
{
	private BTreeMap<Character,Character> bTreeMap;
	private EmbeddedNeo neo;
	private Transaction tx;
	
	@Override
	public void setUp()
	{
		neo = new EmbeddedNeo( "var/map" );
		tx = neo.beginTx();
		Node bNode = neo.createNode();
		bTreeMap = new BTreeMap<Character,Character>( "test_map", bNode, neo );
	}
	
	@Override
	public void tearDown()
	{
 		bTreeMap.delete();
		tx.finish();
		neo.shutdown();
	}
	
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
		tx.success();
	}	
}
