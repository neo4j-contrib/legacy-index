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

import org.neo4j.api.core.Node;
import org.neo4j.util.NeoTestCase;

public class TestBoth extends NeoTestCase
{
    private IndexService indexService;
    private IndexService fulltextIndexService;
    
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        this.indexService = new LuceneIndexService( neo() );
        this.fulltextIndexService = new LuceneFulltextIndexService( neo() );
    }

    @Override
    protected void beforeNeoShutdown()
    {
        super.beforeNeoShutdown();
        this.indexService.shutdown();
        this.fulltextIndexService.shutdown();
    }
    
    public void testSome() throws Exception
    {
        Node node = neo().createNode();
        
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
