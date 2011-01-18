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
package org.neo4j.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

public class TestLuceneFulltextIndexService extends TestLuceneIndexingService
{
    @Override
    protected IndexService instantiateIndex()
    {
        return new LuceneFulltextIndexService( graphDb() );
    }
    
    @Override
    @Ignore
    public void testCaching()
    {
        // Do nothing
    }

    @Override
    @Ignore
    public void testGetNodesBug()
    {
        // Do nothing
    }
    
    @Override
    @Ignore
    public void testRemoveAllWithCache()
    {
        // Do nothing
    }

    @Test
    public void testSimpleFulltext()
    {
        Node node1 = graphDb().createNode();
        
        String value1 = "A value with spaces in it which the fulltext index should tokenize";
        String value2 = "Another value with spaces in it";
        String key = "some_property";
        assertTrue( !index().getNodes( key, value1 ).iterator().hasNext() );
        assertTrue( !index().getNodes( key, value2 ).iterator().hasNext() );

        index().index( node1, key, value1 );
        
        Iterator<Node> itr = index().getNodes( key, "fulltext" ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        
        index().removeIndex( node1, key, value1 );
        assertTrue( !index().getNodes( key, value1 ).iterator().hasNext() );

        index().index( node1, key, value1 );
        Node node2 = graphDb().createNode();
        index().index( node2, key, value1 );
        restartTx();
        
        IndexHits<Node> hits = index().getNodes( key, "tokenize" );
        itr = hits.iterator();
        assertTrue( itr.next() != null );
        assertTrue( itr.next() != null );
        assertTrue( !itr.hasNext() );
        assertTrue( !itr.hasNext() );       
        assertEquals( 2, hits.size() );
        
        index().removeIndex( node1, key, value1 );
        index().removeIndex( node2, key, value1 );
        assertTrue( !index().getNodes( key, value1 ).iterator().hasNext() );
        itr = index().getNodes( key, value1 ).iterator();
        assertTrue( !itr.hasNext() );
        restartTx();
        node1.delete();
        node2.delete();
    }
    
    @Test
    public void testSpecific() throws Exception
    {
        Node andy = graphDb().createNode();
        Node larry = graphDb().createNode();
        String key = "atest";
        index().index( andy, key, "Andy Wachowski" );
        index().index( larry, key, "Larry Wachowski" );
        
        assertCollection( asCollection(
            index().getNodes( key, "andy wachowski" ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "Andy   Wachowski\t  " ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "wachowski larry" ) ), larry );
        assertCollection( asCollection(
            index().getNodes( key, "andy" ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "Andy" ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "larry" ) ), larry );
        assertCollection( asCollection(
            index().getNodes( key, "andy larry" ) ) );
        assertCollection( asCollection(
            index().getNodes( key, "wachowski" ) ), andy, larry );
        assertCollection( asCollection(
            index().getNodes( key, "wachow*" ) ) );
        andy.delete();
        larry.delete();
    }
    
    @Test
    public void testAnotherChangeValueBug() throws Exception
    {
        Node andy = graphDb().createNode();
        Node larry = graphDb().createNode();

        andy.setProperty( "name", "Andy Wachowski" );
        // Deliberately set Larry's name wrong
        larry.setProperty( "name", "Andy Wachowski" );
        index().index( andy, "name", andy.getProperty( "name" ) );
        index().index( larry, "name", larry.getProperty( "name" ) );
        assertCollection( asCollection( index().getNodes(
            "name", "wachowski" ) ), andy, larry );
    
        // Correct Larry's name
        index().removeIndex( larry, "name",
            larry.getProperty( "name" ) );
        larry.setProperty( "name", "Larry Wachowski" );
        index().index( larry, "name",
            larry.getProperty( "name" ) );
    
        assertCollection( asCollection( index().getNodes(
            "name", "wachowski" ) ), andy, larry );
    }    
    
    @Test
    public void testBreakLazyIteratorAfterRemove() throws Exception
    {
        int oldLazyThreshold = ( ( LuceneFulltextIndexService )
            index() ).getLazySearchResultThreshold();
        int lazyThreshold = 5;
        ( ( LuceneFulltextIndexService ) index() ).setLazySearchResultThreshold(
            lazyThreshold );
        List<Node> nodes = new ArrayList<Node>();
        String key = "lazykey";
        String value = "value";
        for ( int i = 0; i < lazyThreshold + 1; i++ )
        {
            Node node = graphDb().createNode();
            nodes.add( node );
            index().index( node, key, value );
        }
        restartTx();
        
        // Assert they're all there
        IndexHits<Node> hits = index().getNodes( key, value );
        assertCollection( asCollection( hits ),
            nodes.toArray( new Node[ 0 ] ) );
        assertEquals( nodes.size(), hits.size() );
        
        // Search again, but don't iterate the result. then remove one node
        // and see how it goes (w/o committing).
        hits = index().getNodes( key, value );
        Node anyNode = nodes.get( nodes.size() - 1 );
        index().removeIndex( anyNode, key, value );
        assertCollection( asCollection( hits ),
            nodes.toArray( new Node[ 0 ] ) );
        assertEquals( nodes.size(), hits.size() );
        restartTx( false );

        // do it again, but this time commit the removal
        hits = index().getNodes( key, value );
        index().removeIndex( anyNode, key, value );
        Node anyOtherNode = nodes.get( nodes.size() - 2 );
        anyOtherNode.delete();
        restartTx();
        // We don't know exactly how lucene does here... if the removedIndex
        // will cause any trouble (if it has been cached in lucene).
        // So just see if we get any exceptions.
        for ( Node hit : hits )
        {
        }
        ( ( LuceneFulltextIndexService ) index() ).setLazySearchResultThreshold(
            oldLazyThreshold );
    }
    
    @Test
    public void testFulltextRemoveAll() throws Exception
    {
        Node node1 = graphDb().createNode();
        Node node2 = graphDb().createNode();
        
        String key = "removeall";
        index().index( node1, key, "value1" );
        index().index( node1, key, "value2" );
        index().index( node2, key, "value1" );
        index().index( node2, key, "value2" );
        assertCollection( asCollection(
            index().getNodes( key, "value1" ) ), node1, node2 );
        index().removeIndex( node1, key );
        assertCollection( asCollection(
            index().getNodes( key, "value1" ) ), node2 );
        assertCollection( asCollection(
            index().getNodes( key, "value2" ) ), node2 );
        
        index().removeIndex( node2, key );
        node2.delete();
        node1.delete();
    }
    
    private LuceneFulltextIndexService fulltextIndex()
    {
        return (LuceneFulltextIndexService) index();
    }
    
    @Test
    public void testExactMatching()
    {
        Node node1 = graphDb().createNode();
        Node node2 = graphDb().createNode();
        String key = "exact";
        index().index( node1, key, "neo4j is great" );
        index().index( node2, key, "lucene is great" );
        assertCollection( index().getNodes( key, "great" ), node1, node2 );
        assertCollection( fulltextIndex().getNodesExactMatch( key, "great" ) );
        assertCollection( fulltextIndex().getNodesExactMatch( key, "neo4j is great" ), node1 );
        assertCollection( fulltextIndex().getNodesExactMatch( key, "lucene is great" ), node2 );
        restartTx();
        assertCollection( index().getNodes( key, "great" ), node1, node2 );
        assertCollection( fulltextIndex().getNodesExactMatch( key, "great" ) );
        assertCollection( fulltextIndex().getNodesExactMatch( key, "neo4j is great" ), node1 );
        assertCollection( fulltextIndex().getNodesExactMatch( key, "lucene is great" ), node2 );
        assertNull( fulltextIndex().getSingleNodeExactMatch( key, "great" ) );
        assertEquals( node1, fulltextIndex().getSingleNodeExactMatch( key, "neo4j is great" ) );
        assertEquals( node2, fulltextIndex().getSingleNodeExactMatch( key, "lucene is great" ) );
        index().removeIndex( key );
        node2.delete();
        node1.delete();
    }
    
    /*
     * This test is just here to get performance numbers on different scenarios:
     * o Do many gets where the transaction is restarted between each get
     * o Do many gets, all in the same transaction
     * o Do many gets, none of them in a transaction
     */
    @Ignore
    @Test
    public void testKjsdk()
    {
        Node node = graphDb().createNode();
        String key = "perf";
        for ( int i = 0; i < 100000; i++ )
        {
            index().index( node, key, i );
            if ( i % 10000 == 0 )
            {
                restartTx();
            }
        }
        restartTx();
        
        int times = 20;
        int count = 10000;
        
        for ( int i = 0; i < times; i++ )
        {
            long t = System.currentTimeMillis();
            for ( int ii = 0; ii < count; ii++ )
            {
                index().getSingleNode( key, 1000 );
                restartTx();
            }
            long total = System.currentTimeMillis() - t;
            System.out.println( "total:" + total );
        }
        
        for ( int i = 0; i < times; i++ )
        {
            long t = System.currentTimeMillis();
            for ( int ii = 0; ii < count; ii++ )
            {
                index().getSingleNode( key, 1000 );
            }
            long totalSameTx = System.currentTimeMillis() - t;
            System.out.println( "total same tx:" + totalSameTx );
        }
        
        finishTx( true );
        for ( int i = 0; i < times; i++ )
        {
            long t = System.currentTimeMillis();
            for ( int ii = 0; ii < count; ii++ )
            {
                index().getSingleNode( key, 1000 );
            }
            long totalNoTx = System.currentTimeMillis() - t;
            System.out.println( "total no tx:" + totalNoTx );
        }
    }
    
    @Override
    protected String dirName()
    {
        return "lucene-fulltext";
    }
}
