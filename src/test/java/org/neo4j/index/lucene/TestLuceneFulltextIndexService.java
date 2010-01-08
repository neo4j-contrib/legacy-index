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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.Isolation;

public class TestLuceneFulltextIndexService extends TestLuceneIndexingService
{
    @Override
    protected IndexService instantiateIndexService()
    {
        return new LuceneFulltextIndexService( neo() );
    }
    
    @Override
    public void testCaching()
    {
        // Do nothing
    }

    @Override
    public void testGetNodesBug()
    {
        // Do nothing
    }

    public void testSimpleFulltext()
    {
        Node node1 = neo().createNode();
        
        String value1 = "A value with spaces in it which the fulltext " +
            "index should tokenize";
        String value2 = "Another value with spaces in it";
        String key = "some_property";
        assertTrue( !indexService().getNodes( key, 
            value1 ).iterator().hasNext() );
        assertTrue( !indexService().getNodes( key, 
            value2 ).iterator().hasNext() );

        indexService().index( node1, key, value1 );
        
        Iterator<Node> itr = indexService().getNodes( key, 
            "fulltext" ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        
        indexService().removeIndex( node1, key, value1 );
        assertTrue( !indexService().getNodes( key, 
            value1 ).iterator().hasNext() );

        indexService().index( node1, key, value1 );
        Node node2 = neo().createNode();
        indexService().index( node2, key, value1 );
        restartTx();
        
        IndexHits hits = indexService().getNodes( key, "tokenize" );
        itr = hits.iterator();
        assertTrue( itr.next() != null );
        assertTrue( itr.next() != null );
        assertTrue( !itr.hasNext() );
        assertTrue( !itr.hasNext() );       
        assertEquals( 2, hits.size() );
        
        indexService().removeIndex( node1, key, value1 );
        indexService().removeIndex( node2, key, value1 );
        assertTrue( !indexService().getNodes( key, 
            value1 ).iterator().hasNext() );
        itr = indexService().getNodes( key, value1 ).iterator();
        assertTrue( !itr.hasNext() );
        restartTx();
        
        indexService().setIsolation( Isolation.ASYNC_OTHER_TX );
        itr = indexService().getNodes( key, value1 ).iterator();
        
        assertTrue( !itr.hasNext() );
        indexService().index( node1, key, value1 );
        itr = indexService().getNodes( key, "tokenize" ).iterator();
        assertTrue( !itr.hasNext() );
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        itr = indexService().getNodes( key, "tokenize" ).iterator();
        assertTrue( itr.hasNext() );
        indexService().setIsolation( Isolation.SYNC_OTHER_TX );
        indexService().removeIndex( node1, key, value1 );
        itr = indexService().getNodes( key, value1 ).iterator();
        assertTrue( !itr.hasNext() );
        node1.delete();
        node2.delete();
    }
    
    public void testSpecific() throws Exception
    {
        Node andy = neo().createNode();
        Node larry = neo().createNode();
        String key = "atest";
        indexService().index( andy, key, "Andy Wachowski" );
        indexService().index( larry, key, "Larry Wachowski" );
        
        assertCollection( asCollection(
            indexService().getNodes( key, "andy wachowski" ) ), andy );
        assertCollection( asCollection(
            indexService().getNodes( key, "Andy   Wachowski\t  " ) ), andy );
        assertCollection( asCollection(
            indexService().getNodes( key, "wachowski larry" ) ), larry );
        assertCollection( asCollection(
            indexService().getNodes( key, "andy" ) ), andy );
        assertCollection( asCollection(
            indexService().getNodes( key, "Andy" ) ), andy );
        assertCollection( asCollection(
            indexService().getNodes( key, "larry" ) ), larry );
        assertCollection( asCollection(
            indexService().getNodes( key, "andy larry" ) ) );
        assertCollection( asCollection(
            indexService().getNodes( key, "wachowski" ) ), andy, larry );
        assertCollection( asCollection(
            indexService().getNodes( key, "wachow*" ) ) );
        andy.delete();
        larry.delete();
    }
    
    public void testAnotherChangeValueBug() throws Exception
    {
        Node andy = neo().createNode();
        Node larry = neo().createNode();

        andy.setProperty( "name", "Andy Wachowski" );
        // Deliberately set Larry's name wrong
        larry.setProperty( "name", "Andy Wachowski" );
        indexService().index( andy, "name", andy.getProperty( "name" ) );
        indexService().index( larry, "name", larry.getProperty( "name" ) );
        assertCollection( asCollection( indexService().getNodes(
            "name", "wachowski" ) ), andy, larry );
    
        // Correct Larry's name
        indexService().removeIndex( larry, "name",
            larry.getProperty( "name" ) );
        larry.setProperty( "name", "Larry Wachowski" );
        indexService().index( larry, "name",
            larry.getProperty( "name" ) );
    
        assertCollection( asCollection( indexService().getNodes(
            "name", "wachowski" ) ), andy, larry );
    }    
    
    public void testBreakLazyIteratorAfterRemove() throws Exception
    {
        int oldLazyThreshold = ( ( LuceneIndexService )
            indexService() ).getLazySearchResultThreshold();
        int lazyThreshold = 5;
        ( ( LuceneIndexService ) indexService() ).setLazySearchResultThreshold(
            lazyThreshold );
        List<Node> nodes = new ArrayList<Node>();
        String key = "lazykey";
        String value = "value";
        for ( int i = 0; i < lazyThreshold + 1; i++ )
        {
            Node node = neo().createNode();
            nodes.add( node );
            indexService().index( node, key, value );
        }
        restartTx();
        
        // Assert they're all there
        IndexHits<Node> hits = indexService().getNodes( key, value );
        assertCollection( asCollection( hits ),
            nodes.toArray( new Node[ 0 ] ) );
        assertEquals( nodes.size(), hits.size() );
        
        // Search again, but don't iterate the result. then remove one node
        // and see how it goes (w/o committing).
        hits = indexService().getNodes( key, value );
        Node anyNode = nodes.get( nodes.size() - 1 );
        indexService().removeIndex( anyNode, key, value );
        assertCollection( asCollection( hits ),
            nodes.toArray( new Node[ 0 ] ) );
        assertEquals( nodes.size(), hits.size() );
        restartTx( false );

        // do it again, but this time commit the removal
        hits = indexService().getNodes( key, value );
        indexService().removeIndex( anyNode, key, value );
        Node anyOtherNode = nodes.get( nodes.size() - 2 );
        anyOtherNode.delete();
        restartTx();
        // We don't know exactly how lucene does here... if the removedIndex
        // will cause any trouble (if it has been cached in lucene).
        // So just see if we get any exceptions.
        for ( Node hit : hits )
        {
        }
        ( ( LuceneIndexService ) indexService() ).setLazySearchResultThreshold(
            oldLazyThreshold );
    }
}
