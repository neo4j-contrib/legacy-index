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
package org.neo4j.util.index;

import java.util.Iterator;

import org.neo4j.api.core.DynamicRelationshipType;
import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;

public class TestNeoIndexingService extends NeoTestCase
{
	private IndexService indexService;
	
	@Override
	public void setUp() throws Exception
	{
	    super.setUp();
        indexService = new NeoIndexService( neo() );
	}
	
	@Override
	protected void beforeNeoShutdown()
	{
        indexService.shutdown();
	}
    
    public void testSimple()
    {
        Node node1 = neo().createNode();
        
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
        Node node2 = neo().createNode();
        indexService.index( node2, "a_property", 1 );
        
        itr = indexService.getNodes( "a_property", 1 ).iterator();
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
    
    public void testProgramaticNeoMultiCommit() 
    {
        String testValue = String.valueOf(System.nanoTime());
        assertTrue("No node expected", !hasNewNode(testValue));

        insertNewNodeAndCommit(testValue);
        assertTrue("Node expected", hasNewNode(testValue));

        testValue = String.valueOf(System.nanoTime());
        assertTrue("No node expected", !hasNewNode(testValue));

        insertNewNodeAndCommit(testValue);
        assertTrue("Node expected", hasNewNode(testValue));
    }
    
    private void insertNewNodeAndCommit( String testValue )
    {
        restartTx();
        try 
        {
            Node newNode = neo().createNode();
            newNode.setProperty("id", testValue);
            neo().getReferenceNode().createRelationshipTo(newNode,
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
            Node newNode = neo().createNode();
            newNode.setProperty("id", testValue);
            neo().getReferenceNode().createRelationshipTo(newNode,
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
		Node node1 = neo.createNode();
		try 
		{ 
			new MultiValueIndex( "blabla", null, neo );
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
		Index sIndex = new SingleValueIndex( "multi", node1, neo );
		try 
		{ 
			new MultiValueIndex( "blabla", node1, neo );
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
            Node node1 = neo.createNode();
            Node node2 = neo.createNode();
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
