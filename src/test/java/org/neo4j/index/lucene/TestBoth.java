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

import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexService;
import org.neo4j.index.Neo4jTestCase;

public class TestBoth extends Neo4jTestCase
{
    private IndexService indexService;
    private IndexService fulltextIndexService;
    
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        this.indexService = new LuceneIndexService( graphDb() );
        this.fulltextIndexService = new LuceneFulltextIndexService( graphDb() );
    }

    @Override
    protected void beforeShutdown()
    {
        super.beforeShutdown();
        this.indexService.shutdown();
        this.fulltextIndexService.shutdown();
    }
    
    public void testSome() throws Exception
    {
        Node node = graphDb().createNode();
        
        String key = "some_key";
        String value1 = "347384738-2";
        String value2 = "Tjena hej hoj";
        this.indexService.index( node, key, value1 );
        this.fulltextIndexService.index( node, key, value2 );
        
        restartTx();
        
        assertEquals( node, this.indexService.getSingleNode( key, value1 ) );
        assertEquals( node,
            this.fulltextIndexService.getSingleNode( key, "Tjena" ) );
        
        this.indexService.removeIndex( node, "cpv", value1 );
        this.fulltextIndexService.removeIndex( node, "cpv-ft", value2 );
        node.delete();
    }
}
