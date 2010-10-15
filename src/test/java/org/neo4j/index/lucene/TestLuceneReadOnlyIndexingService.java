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

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.Neo4jWithIndexTestCase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;

public class TestLuceneReadOnlyIndexingService extends Neo4jWithIndexTestCase
{
    @Override
    protected IndexService instantiateIndex()
    {
        return new LuceneIndexService( graphDb() );
    }
	
    @Test
    public void testSimple()
    {
        Node node1 = graphDb().createNode();
        
        assertTrue( !index().getNodes( "a_property", 1 ).iterator().hasNext() );
        assertEquals( 0, index().getNodes( "a_property", 1 ).size() );

        index().index( node1, "a_property", 1 );
        
        IndexHits<Node> hits = index().getNodes( "a_property", 1 );
        Iterator<Node> itr = hits.iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        assertEquals( 1, hits.size() );
        restartTx();
        
        GraphDatabaseService readOnlyGraphDb = new EmbeddedReadOnlyGraphDatabase(
            getDbPath().getAbsolutePath() );
        IndexService readOnlyIndex = new LuceneReadOnlyIndexService( readOnlyGraphDb );
        Transaction tx = readOnlyGraphDb.beginTx();
        itr = readOnlyIndex.getNodes( "a_property", 1 ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        tx.finish();
        readOnlyIndex.shutdown();
        readOnlyGraphDb.shutdown();
        
        index().removeIndex( node1, "a_property", 1 );
        node1.delete();
    }
    
    @Test
    public void testReadOnlyWithNoTransaction()
    {
        Node node1 = graphDb().createNode();
        index().index( node1, "a_property", 1 );
        IndexHits<Node> hits = index().getNodes( "a_property", 1 );
        Iterator<Node> itr = hits.iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        assertEquals( 1, hits.size() );
        finishTx( true );
        
        try
        {
            index().index( node1, "a_property", 2 );
            fail( "Write operation with no transaction should fail" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            index().removeIndex( node1, "a_property", 1 );
            fail( "Write operation with no transaction should fail" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        
        //now try read operation without tx
        hits = index().getNodes( "a_property", 1 );
        itr = hits.iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        assertEquals( 1, hits.size() );
    }
}
