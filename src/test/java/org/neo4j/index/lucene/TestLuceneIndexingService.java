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
package org.neo4j.index.lucene;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.Isolation;
import org.neo4j.index.Neo4jTestCase;

public class TestLuceneIndexingService extends Neo4jTestCase
{
	private IndexService indexService;
	
	protected IndexService instantiateIndexService()
	{
	    return new LuceneIndexService( graphDb() );
	}
	
	@Override
	protected void setUp() throws Exception
	{
	    super.setUp();
        indexService = instantiateIndexService();
	}
	
	protected IndexService indexService()
	{
	    return indexService;
	}
	
	@Override
	protected void beforeShutdown()
	{
	    indexService().shutdown();
	}
	
    public void testSimple()
    {
        Node node1 = graphDb().createNode();
        
        assertTrue( !indexService().getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService().index( node1, "a_property", 1 );
        
        IndexHits<Node> hits = indexService().getNodes( "a_property", 1 );
        Iterator<Node> itr = hits.iterator();
        assertEquals( node1, itr.next() );
        assertEquals( 1, hits.size() );
        assertTrue( !itr.hasNext() );
        
        indexService().removeIndex( node1, "a_property", 1 );
        assertTrue( !indexService().getNodes( "a_property", 
            1 ).iterator().hasNext() );

        indexService().index( node1, "a_property", 1 );
        Node node2 = graphDb().createNode();
        indexService().index( node2, "a_property", 1 );
        
        hits = indexService().getNodes( "a_property", 1 );
        itr = hits.iterator();
        assertTrue( itr.next() != null );
        assertTrue( itr.next() != null );
        assertTrue( !itr.hasNext() );
        assertTrue( !itr.hasNext() );       
        assertEquals( 2, hits.size() );
        
        indexService().removeIndex( node1, "a_property", 1 );
        indexService().removeIndex( node2, "a_property", 1 );
        assertTrue( !indexService().getNodes( "a_property", 
            1 ).iterator().hasNext() );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        restartTx();
        
        indexService().setIsolation( Isolation.ASYNC_OTHER_TX );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        
        assertTrue( !itr.hasNext() );
        indexService().index( node1, "a_property", 1 );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( itr.hasNext() );
        indexService().setIsolation( Isolation.SYNC_OTHER_TX );
        indexService().removeIndex( node1, "a_property", 1 );
        itr = indexService().getNodes( "a_property", 1 ).iterator();
        assertTrue( !itr.hasNext() );
        node1.delete();
        node2.delete();
    }
    
    public void testMultipleAdd()
    {
        Node node = graphDb().createNode();
        indexService().index( node, "a_property", 3 );
        restartTx();
        indexService().index( node, "a_property", 3 );
        restartTx();
        indexService().removeIndex( node, "a_property", 3 );
        restartTx();
        assertTrue( indexService().getSingleNode( "a_property", 3 ) == null );
    }
    
    public void testCaching()
    {
        String key = "prop";
        Object value = 10;
        assertNull( ( (LuceneIndexService) indexService() ).getEnabledCacheSize( key ) );
        ( ( LuceneIndexService ) indexService() ).enableCache( key, 1000 );
        assertEquals( ( Integer ) 1000,
                ( (LuceneIndexService) indexService() ).getEnabledCacheSize( key ) );
        Node node1 = graphDb().createNode();
        indexService().index( node1, key, value );
        indexService().getNodes( key, value );
        restartTx( false );
        
        Node node2 = graphDb().createNode();
        indexService().getNodes( key, value );
        indexService().index( node2, key, value );
        indexService().getNodes( key, value );
        restartTx();
        
        Node node3 = graphDb().createNode();
        indexService().getNodes( key, value );
        indexService().index( node3, key, value );
        indexService().getNodes( key, value );
        restartTx( false );
        
        Node node4 = graphDb().createNode();
        indexService().getNodes( key, value );
        indexService().index( node4, key, value );
        indexService().getNodes( key, value );
        restartTx();
        
        assertCollection( asCollection( indexService().getNodes( key, value ) ),
            node2, node4 );
        
        indexService().removeIndex( node2, key, value );
        indexService().removeIndex( node4, key, value );
        node2.delete();
        node4.delete();
    }
    
    public void testRollback()
    {
        Node node1 = graphDb().createNode();
        Node node2 = graphDb().createNode();
        restartTx();
        indexService().index( node1, "a_property", 3 );
        assertEquals( node1, indexService().getSingleNode( "a_property", 3 ) );
        restartTx( false );
        assertEquals( null, indexService().getSingleNode( "a_property", 3 ) );
        indexService().index( node2, "a_property", 3 );
        assertEquals( node2, indexService().getSingleNode( "a_property", 3 ) );
        restartTx();
        assertEquals( node2, indexService().getSingleNode( "a_property", 3 ) );
    }
    
    public void testDoubleIndexing() throws Exception
    {
        Node node = graphDb().createNode();
        indexService.index( node, "double", "value" );
        restartTx();
        indexService.index( node, "double", "value" );
        restartTx();
        Set<Node> hits = new HashSet<Node>();
        for ( Node hit : indexService.getNodes( "double", "value" ) )
        {
            assertTrue( hits.add( hit ) );
        }
        node.delete();
    }
    
    public void testChangeValueBug() throws Exception
    {
        Node andy = graphDb().createNode();
        Node larry = graphDb().createNode();

        andy.setProperty( "name", "Andy Wachowski" );
        andy.setProperty( "title", "Director" );
        // Deliberately set Larry's name wrong
        larry.setProperty( "name", "Andy Wachowski" );
        larry.setProperty( "title", "Director" );
        indexService().index( andy, "name", andy.getProperty( "name" ) );
        indexService().index( andy, "title", andy.getProperty( "title" ) );
        indexService().index( larry, "name", larry.getProperty( "name" ) );
        indexService().index( larry, "title", larry.getProperty( "title" ) );
    
        // Correct Larry's name
        indexService().removeIndex( larry, "name",
            larry.getProperty( "name" ) );
        larry.setProperty( "name", "Larry Wachowski" );
        indexService().index( larry, "name",
            larry.getProperty( "name" ) );
    
        assertCollection( asCollection( indexService().getNodes(
            "name", "Andy Wachowski" ) ), andy );
        assertCollection( asCollection( indexService().getNodes(
            "name", "Larry Wachowski" ) ), larry );
    }
    
    public void testGetNodesBug() throws Exception
    {
        Node node1 = graphDb().createNode();
        Node node2 = graphDb().createNode();
        String key = "getnodesbug";
        ( ( LuceneIndexService ) indexService() ).enableCache( key, 100 );
        indexService().index( node1, key, "value" );
        indexService().index( node2, key, "value" );
        restartTx();
        
        assertCollection( asCollection(
            indexService().getNodes( key, "value" ) ), node1, node2 );
        // Now that value is cached
        indexService().removeIndex( node1, key, "value" );
        assertCollection( asCollection(
            indexService().getNodes( key, "value" ) ), node2 );
        indexService().removeIndex( node2, key, "value" );
        node1.delete();
        node2.delete();
    }
    
    public void testRemoveAll() throws Exception
    {
        Node node1 = graphDb().createNode();
        Node node2 = graphDb().createNode();
        
        String key = "removeall";
        indexService().index( node1, key, "value1" );
        indexService().index( node1, key, "value2" );
        indexService().index( node2, key, "value1" );
        indexService().index( node2, key, "value2" );
        restartTx();
        assertCollection( asCollection(
            indexService().getNodes( key, "value1" ) ), node1, node2 );
        indexService().removeIndex( node1, key );
        indexService().index( node1, key, "value2" );
        assertCollection( asCollection(
            indexService().getNodes( key, "value1" ) ), node2 );
        assertCollection( asCollection(
            indexService().getNodes( key, "value2" ) ), node1, node2 );
        indexService().index( node1, key, "value1" );
        restartTx();
        indexService().removeIndex( key );
        assertCollection( asCollection(
            indexService().getNodes( key, "value2" ) ) );
        indexService().index( node1, key, "value1" );
        indexService().index( node2, key, "value2" );
        assertCollection( asCollection(
            indexService().getNodes( key, "value1" ) ), node1 );
        restartTx();
        indexService().removeIndex( key );
        assertCollection( asCollection(
            indexService().getNodes( key, "value1" ) ) );
        assertCollection( asCollection(
            indexService().getNodes( key, "value2" ) ) );
        
        node2.delete();
        node1.delete();
    }
    
    public void testRemoveAllWithCache() throws Exception
    {
        Node node1 = graphDb().createNode();
        String key = "removeallc";
        ( (LuceneIndexService) indexService() ).enableCache( key, 1000 );
        indexService().index( node1, key, "value1" );
        indexService().index( node1, key, "value2" );
        restartTx();
        assertEquals( node1, indexService().getSingleNode( key, "value1" ) );
        assertEquals( node1, indexService().getSingleNode( key, "value2" ) );
        indexService().removeIndex( node1, key );
        assertNull( indexService().getSingleNode( key, "value1" ) );
        assertNull( indexService().getSingleNode( key, "value2" ) );
        restartTx();
        assertNull( indexService().getSingleNode( key, "value1" ) );
        assertNull( indexService().getSingleNode( key, "value2" ) );
        node1.delete();
    }
    
    public void testIndexLargeString() throws Exception
    {
            Node node1 = graphDb().createNode();
            byte[] data = new byte[10*1024*1024];
            String value = new String( data );
            indexService().index( node1, "large_string", value );
            restartTx();
//          this will not work            
//            assertEquals( node1, 
//                    indexService().getSingleNode( "large_string", value ) );
            indexService.removeIndex( node1, "large_string" );
            node1.delete();
    }
    
//    public void testDifferentTypesWithSameValueIssue()
//    {
//        String key = "prop";
//        Integer valueAsInt = 10;
//        String valueAsString = "10";
//        
//        Node node1 = graphDb().createNode();
//        indexService.index( node1, key, valueAsInt );
//        Node node2 = graphDb().createNode();
//        indexService.index( node2, key, valueAsString );
//        
//        assertCollection( indexService.getNodes( key, valueAsInt ), node1 );
//        assertCollection( indexService.getNodes( key, valueAsString ), node2 );
//        
//        tx.success(); tx.finish(); tx = graphDb().beginTx();
//        
//        assertCollection( indexService.getNodes( key, valueAsInt ), node1 );
//        assertCollection( indexService.getNodes( key, valueAsString ), node2 );
//        
//        indexService.removeIndex( node1, key, valueAsInt );
//        indexService.removeIndex( node2, key, valueAsString );
//        
//        node2.delete();
//        node1.delete();
//    }
}
