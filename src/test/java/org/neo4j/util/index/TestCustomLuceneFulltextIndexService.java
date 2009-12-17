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
package org.neo4j.util.index;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.neo4j.api.core.Node;

public class TestCustomLuceneFulltextIndexService
    extends TestLuceneIndexingService
{
    @Override
    protected IndexService instantiateIndexService()
    {
        return new LuceneFulltextIndexService( neo() )
        {
            @Override
            protected Query formQuery( String key, Object value )
            {
                try
                {
                    return new QueryParser( DOC_INDEX_KEY,
                        new WhitespaceAnalyzer() ).parse( value.toString() );
                }
                catch ( ParseException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }
    
    @Override
    public void testCaching()
    {
    }
    
    public void testCustomFulltext() throws Exception
    {
        Node node1 = neo().createNode();
        Node node2 = neo().createNode();
        
        String key1 = "lastName";
        String key2 = "modifiedTime";

        indexService().index( node1, key1, "Smith" );
        indexService().index( node2, key1, "Mattias Smith" );
        indexService().index( node2, key2, "2009" );
        indexService().index( node1, key2, "449854" );
        
        assertCollection( asCollection(
            indexService().getNodes( key1, "smith" ) ), node1, node2 );
        assertCollection( asCollection(
            indexService().getNodes( key1, "smish~" ) ), node1, node2 );
        assertCollection( asCollection(
            indexService().getNodes( key2, "[2010 TO >]" ) ), node1 );
        
        restartTx();
        
        assertCollection( asCollection(
            indexService().getNodes( key1, "smith" ) ), node1, node2 );
        assertCollection( asCollection(
            indexService().getNodes( key1, "smish~" ) ), node1, node2 );
        assertCollection( asCollection(
            indexService().getNodes( key2, "[2010 TO >]" ) ), node1 );
    }
}
