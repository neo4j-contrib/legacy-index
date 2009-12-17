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

import org.neo4j.api.core.Node;

public class TestCustomLuceneFulltextIndexService
    extends TestLuceneIndexingService
{
    @Override
    protected IndexService instantiateIndexService()
    {
        return new LuceneFulltextQueryIndexService( neo() );
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
            indexService().getNodes( key, "Andy Wachowski" ) ), andy );
        assertCollection( asCollection(
            indexService().getNodes( key, "andy" ) ), andy );
        assertCollection( asCollection(
            indexService().getNodes( key, "Andy" ) ), andy );
        assertCollection( asCollection(
            indexService().getNodes( key, "larry" ) ), larry );
        assertCollection( asCollection(
            indexService().getNodes( key, "andy larry" ) ), andy, larry );
        assertCollection( asCollection(
            indexService().getNodes( key, "andy AND larry" ) ) );
        assertCollection( asCollection(
            indexService().getNodes( key, "andy OR Larry" ) ) );
        assertCollection( asCollection(
            indexService().getNodes( key, "wachow*" ) ), andy, larry );
    }
}
