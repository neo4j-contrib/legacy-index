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

import java.util.Iterator;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.Isolation;
import org.neo4j.index.Neo4jTestCase;

public class TestNeoIndexingService extends Neo4jTestCase
{
	private IndexService indexService;
	
	@Override
	public void setUp() throws Exception
	{
	    super.setUp();
        indexService = new NeoIndexService( graphDb() );
	}
	
	@Override
	protected void beforeShutdown()
	{
        indexService.shutdown();
	}
    
    public void testSimple()
    {
        Node node1 = graphDb().createNode();
        
        assertTrue( !indexService.getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService.index( node1, "a_property", 1 );
        
        Iterator<Node> itr = indexService.getNodes( "a_property", 
            1 ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        
        indexService.removeIndex( node1, "a_property", 1 );
        assertTrue( !indexService.getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService.index( node1, "a_property", 1 );
        Node node2 = graphDb().createNode();
        indexService.index( node2, "a_property", 1 );
        
        IndexHits hits = indexService.getNodes( "a_property", 1 );
        assertEquals( 2, hits.size() );
        itr = hits.iterator();
        assertTrue( itr.next() != null );
        assertTrue( itr.next() != null );
        assertTrue( !itr.hasNext() );
        assertTrue( !itr.hasNext() );       
        
        indexService.removeIndex( node1, "a_property", 1 );
        indexService.removeIndex( node2, "a_property", 1 );
        assertTrue( !indexService.getNodes( "a_property", 
            1 ).iterator().hasNext() );
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        restartTx();
        
        indexService.setIsolation( Isolation.ASYNC_OTHER_TX );
        indexService.index( node1, "a_property", 1 );
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( itr.hasNext() );
        indexService.setIsolation( Isolation.SYNC_OTHER_TX );
        indexService.removeIndex( node1, "a_property", 1 );
        itr = indexService.getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        node1.delete();
        node2.delete();
    }
    
    public void testProgramaticGraphDbMultiCommit() 
    {
        String testValue = String.valueOf(System.nanoTime());
        assertTrue("No node expected", !hasNewNode(testValue));

        insertNewNodeAndCommit(testValue);
        assertTrue("Node expected", hasNewNode(testValue));

        testValue = String.valueOf(System.nanoTime());
        assertTrue("No node expected", !hasNewNode(testValue));

        insertNewNodeAndCommit(testValue);
        assertTrue("Node expected", hasNewNode(testValue));

        testValue = String.valueOf(System.nanoTime());
        assertTrue("No node expected", !hasNewNode(testValue));

        insertNewNodeAndRollback(testValue);
        assertTrue("No node expected", !hasNewNode(testValue));
    }
    
    private void insertNewNodeAndCommit( String testValue )
    {
        restartTx();
        try 
        {
            Node newNode = graphDb().createNode();
            newNode.setProperty("id", testValue);
            graphDb().getReferenceNode().createRelationshipTo(newNode,
                DynamicRelationshipType.withName( "LINKS_TO" ));
            indexService.index(newNode, "id", testValue);
            restartTx( true );
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }        
    }
	
    private void insertNewNodeAndRollback( String testValue )
    {
        restartTx();
        try 
        {
            Node newNode = graphDb().createNode();
            newNode.setProperty("id", testValue);
            graphDb().getReferenceNode().createRelationshipTo(newNode,
                DynamicRelationshipType.withName( "LINKS_TO" ));
            indexService.index(newNode, "id", testValue);
            restartTx( false );
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }        
    }

    private boolean hasNewNode(String testValue) 
    {
        restartTx();
        Node node = indexService.getSingleNode("id", testValue);
        restartTx( true );
        return node != null;
    }
    
/*	public void testIllegalStuff()
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
		tx.success();
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
    }*/
}
