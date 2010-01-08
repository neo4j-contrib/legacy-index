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

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.NeoTestCase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;

public class TestLuceneReadOnlyIndexingService extends NeoTestCase
{
	private IndexService indexService;
	
	protected IndexService instantiateIndexService()
	{
	    return new LuceneIndexService( neo() );
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
	protected void beforeNeoShutdown()
	{
	    indexService().shutdown();
	}
	
    public void testSimple()
    {
        Node node1 = neo().createNode();
        
        assertTrue( !indexService().getNodes( "a_property", 
            1 ).iterator().hasNext() );
        assertEquals( 0, indexService().getNodes( "a_property", 1 ).size() );

        indexService().index( node1, "a_property", 1 );
        
        IndexHits hits = indexService().getNodes( "a_property", 1 );
        Iterator<Node> itr = hits.iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        assertEquals( 1, hits.size() );
        restartTx();
        
        GraphDatabaseService readOnlyNeo = new EmbeddedReadOnlyGraphDatabase(
            getNeoPath().getAbsolutePath() );
        IndexService readOnlyIndex = new LuceneReadOnlyIndexService( readOnlyNeo );
        Transaction tx = readOnlyNeo.beginTx();
        itr = readOnlyIndex.getNodes( "a_property", 1 ).iterator();
        assertEquals( node1, itr.next() );
        assertTrue( !itr.hasNext() );
        tx.finish();
        readOnlyIndex.shutdown();
        readOnlyNeo.shutdown();
        
        indexService().removeIndex( node1, "a_property", 1 );
        node1.delete();
    }
}
