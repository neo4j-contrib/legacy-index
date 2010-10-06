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

package org.neo4j.index.lucene;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexService;
import org.neo4j.index.Neo4jWithIndexTestCase;

public abstract class TestLuceneIndexLazyness extends Neo4jWithIndexTestCase
{
    @Override
    protected IndexService instantiateIndex()
    {
        return new LuceneIndexService( graphDb() );
    }
    
    @Test
    public void testIt() throws Exception
    {
        String key = "mykey";
        String value = "myvalue";
        Collection<Node> nodes = new ArrayList<Node>();
        for ( int i = 0; i < 20000; i++ )
        {
            Node node = graphDb().createNode();
            index().index( node, key, value );
            nodes.add( node );
            if ( i == 2000 )
            {
                Iterable<Node> itr = index().getNodes( key, value );
                assertCollection( asCollection( itr ),
                    nodes.toArray( new Node[ 0 ] ) );
            }
            
            if ( i % 10000 == 0 )
            {
                restartTx();
            }
        }
        restartTx();
        
        Node[] nodeArray = nodes.toArray( new Node[ 0 ] );
        long total = 0;
        long totalTotal = 0;
        int counter = 0;
        for ( int i = 0; i < 10; i++ )
        {
            // So that it'll get the nodes in the synchronous way
            ( ( LuceneIndexService ) index() ).
                setLazySearchResultThreshold( nodes.size() + 10 );
            long time = System.currentTimeMillis();
            Iterable<Node> itr = index().getNodes( key, value );
            long syncTime = System.currentTimeMillis() - time;
            assertCollection( asCollection( itr ), nodeArray );
            long syncTotalTime = System.currentTimeMillis() - time;
            
            // So that it'll get the nodes in the lazy way
            ( ( LuceneIndexService ) index() ).
                setLazySearchResultThreshold( nodes.size() - 10 );
            time = System.currentTimeMillis();
            itr = index().getNodes( key, value );
            long lazyTime = System.currentTimeMillis() - time;
            assertCollection( asCollection( itr ), nodeArray );
            long lazyTotalTime = System.currentTimeMillis() - time;
//            System.out.println( "lazy:" + lazyTime + " (" + lazyTotalTime +
//                "), sync:" + syncTime + " (" + syncTotalTime + ")" );
            
            if ( i > 0 )
            {
                total += syncTime;
                totalTotal += syncTotalTime;
                counter++;
            }
            
            // At the very least
            assertTrue( lazyTime < syncTime / 3 );
        }
        
//        System.out.println( "avg:" + ( total / counter ) + ", " +
//            ( totalTotal / counter ) );
        
        for ( Node node : nodes )
        {
            node.delete();
        }
    }
}
