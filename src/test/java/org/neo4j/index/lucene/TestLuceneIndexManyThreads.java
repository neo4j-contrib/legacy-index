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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.Neo4jWithIndexTestCase;

/**
 * This test is abstract because it takes a while to run. It belongs in a QA project
 * instead really...
 */
public abstract class TestLuceneIndexManyThreads extends Neo4jWithIndexTestCase
{
    private AtomicInteger COUNT_CREATES = new AtomicInteger();
    private AtomicInteger COUNT_DELETES = new AtomicInteger();
    private AtomicInteger COUNT_READS = new AtomicInteger();
    
    @Override
    protected IndexService instantiateIndex()
    {
        return new LuceneIndexService( graphDb() );
    }
    
    @Test
    public void tryToBreak() throws Exception
    {
        Node rootNode = graphDb().createNode();
        restartTx();
        
        Collection<WorkerThread> threads = new ArrayList<WorkerThread>();
        long endTime = System.currentTimeMillis() + 1000 * 30;
        List<Long> aliveNodes = Collections.synchronizedList(
            new ArrayList<Long>() );
        for ( int i = 0; i < 20; i++ )
        {
            WorkerThread thread =
                new WorkerThread( rootNode, endTime, aliveNodes );
            threads.add( thread );
            thread.start();
        }
        for ( WorkerThread thread : threads )
        {
            thread.join();
            if ( thread.exception != null )
            {
                thread.exception.printStackTrace();
                fail( thread.exception.getMessage() );
            }
        }
//        System.out.println( "c:" + COUNT_CREATES.get() + ", d:" +
//            COUNT_DELETES.get() + ", r:" + COUNT_READS.get() );
    }
    
    private enum RelTypes implements RelationshipType
    {
        TEST_TYPE,
    }
    
    private class WorkerThread extends Thread
    {
        private final Random random = new Random();
        private final Node rootNode;
        private final long endTime;
        private final List<Long> aliveNodes;
        private RuntimeException exception;
        
        WorkerThread( Node rootNode, long endTime, List<Long> aliveNodes )
        {
            this.rootNode = rootNode;
            this.endTime = endTime;
            this.aliveNodes = aliveNodes;
        }
        
        @Override
        public void run()
        {
            try
            {
            while ( System.currentTimeMillis() < endTime )
            {
                Collection<Long> createdIds = null;
                Collection<Long> deletedIds = null;
                Transaction tx = graphDb().beginTx();
                try
                {
                    int what = random.nextInt( 3 );
                    if ( what == 0 ) // Create stuff
                    {
                        createdIds = createStuff();
                    }
                    else if ( what == 1 ) // Delete stuff
                    {
                        deletedIds = deleteStuff();
                    }
                    else if ( what == 2 ) // Verify stuff
                    {
                        verifyStuff();
                    }
                    tx.success();
                }
                finally
                {
                    tx.finish();
                }
                
                if ( createdIds != null )
                {
                    aliveNodes.addAll( createdIds );
                    COUNT_CREATES.addAndGet( createdIds.size() );
                }
                if ( deletedIds != null )
                {
                    aliveNodes.removeAll( deletedIds );
                    COUNT_DELETES.addAndGet( deletedIds.size() );
                }
            }
            }
            catch ( RuntimeException e )
            {
                this.exception = e;
                throw e;
            }
        }
        
        private void set( Node node, String key, Object value )
        {
            node.setProperty( key, value );
            index().index( node, key, value );
        }

        private Collection<Long> createStuff()
        {
            int count = random.nextInt( 1000 ) + 1;
            Collection<Long> ids = new ArrayList<Long>();
            for ( int i = 0; i < count; i++ )
            {
                Node node = graphDb().createNode();
                rootNode.createRelationshipTo( node, RelTypes.TEST_TYPE );
                set( node, "type", "TYPE" );
                set( node, "name", "user" + random.nextInt( 10000 ) );
                if ( random.nextBoolean() )
                {
                    set( node, "sometimes", random.nextInt( 10 ) );
                }
                ids.add( node.getId() );
            }
            return ids;
        }
        
        private Collection<Long> deleteStuff()
        {
            if ( aliveNodes.size() < 2000 )
            {
                return Collections.emptySet();
            }
            
            int count = random.nextInt( 3 ) + 1;
            Collection<Long> ids = new ArrayList<Long>();
            for ( int i = 0; i < count; i++ )
            {
                Node node = getRandomAliveNode();
                if ( node == null )
                {
                    continue;
                }
                for ( String key : node.getPropertyKeys() )
                {
                    index().removeIndex( node, key,
                        node.getProperty( key ) );
                }
                node.getSingleRelationship( RelTypes.TEST_TYPE,
                    Direction.INCOMING ).delete();
                node.delete();
                ids.add( node.getId() );
            }
            return ids;
        }
        
        private Node getRandomAliveNode()
        {
            if ( aliveNodes.isEmpty() )
            {
                return null;
            }
            return graphDb().getNodeById( aliveNodes.get( random.nextInt(
                aliveNodes.size() ) ) );
        }
        
        private void verifyStuff()
        {
            // Test to get an iterator for the "type" property
            // (all nodes have this)
//            if ( aliveNodes.size() > 3000 )
//            {
//                IndexHits<Node> sometimesHits = indexService.getNodes(
//                    "sometimes", 5 );
//                assertTrue( isRoughly( aliveNodes.size() / 20,
//                    sometimesHits.size() ) );
//            }
            IndexHits<Node> hits = index().getNodes( "type", "TYPE" );
            for ( Node hit : hits )
            {
                hit.getProperty( "name" );
                COUNT_READS.incrementAndGet();
            }
            
            // Test one random node
            Node node = getRandomAliveNode();
            if ( node != null )
            {
                String name = ( String ) node.getProperty( "name" );
                boolean found = false;
                for ( Node hit : index().getNodes( "name", name ) )
                {
                    COUNT_READS.incrementAndGet();
                    if ( hit.equals( node ) )
                    {
                        found = true;
                    }
                }
                assertTrue( found );
            }
        }

//        private boolean isRoughly( int shouldBeRoughly, int value )
//        {
//            // Ok so within +-20%
//            int max = ( int ) ( shouldBeRoughly * 1.2d );
//            int min = ( int ) ( shouldBeRoughly * 0.8d );
//            return value >= min && value <= max;
//        }
    }
}
