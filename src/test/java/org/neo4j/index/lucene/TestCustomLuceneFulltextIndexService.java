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

import org.junit.Ignore;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexService;

public class TestCustomLuceneFulltextIndexService
    extends TestLuceneIndexingService
{
    @Override
    protected IndexService instantiateIndex()
    {
        return new LuceneFulltextQueryIndexService( graphDb() );
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
    
    public void testCustomFulltext() throws Exception
    {
        Node node1 = graphDb().createNode();
        Node node2 = graphDb().createNode();
        
        String key1 = "lastName";
        String key2 = "modifiedTime";

        index().index( node1, key1, "Smith" );
        index().index( node2, key1, "Mattias Smith" );
        index().index( node2, key2, "2009" );
        index().index( node1, key2, "449854" );
        
        assertCollection( asCollection(
            index().getNodes( key1, "smith" ) ), node1, node2 );
        assertCollection( asCollection(
            index().getNodes( key1, "smish~" ) ), node1, node2 );
        assertCollection( asCollection(
            index().getNodes( key2, "[2010 TO >]" ) ), node1 );
        
        restartTx();
        
        assertCollection( asCollection(
            index().getNodes( key1, "smith" ) ), node1, node2 );
        assertCollection( asCollection(
            index().getNodes( key1, "smish~" ) ), node1, node2 );
        assertCollection( asCollection(
            index().getNodes( key2, "[2010 TO >]" ) ), node1 );
    }

    public void testSpecific() throws Exception
    {
        Node andy = graphDb().createNode();
        Node larry = graphDb().createNode();
        String key = "atest";
        index().index( andy, key, "Andy Wachowski" );
        index().index( larry, key, "Larry Wachowski" );
        
        assertCollection( asCollection(
            index().getNodes( key, "+andy +wachowski" ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "+Andy +Wachowski" ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "andy" ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "Andy" ) ), andy );
        assertCollection( asCollection(
            index().getNodes( key, "larry" ) ), larry );
        assertCollection( asCollection(
            index().getNodes( key, "andy larry" ) ), andy, larry );
        assertCollection( asCollection(
            index().getNodes( key, "+andy +larry" ) ) );
        assertCollection( asCollection(
            index().getNodes( key, "wachow*" ) ), andy, larry );
    }

    @Override
    public void testChangeValueBug() throws Exception
    {
        Node andy = graphDb().createNode();
        Node larry = graphDb().createNode();

        andy.setProperty( "name", "Andy Wachowski" );
        andy.setProperty( "title", "Director" );
        // Deliberately set Larry's name wrong
        larry.setProperty( "name", "Andy Wachowski" );
        larry.setProperty( "title", "Director" );
        index().index( andy, "name", andy.getProperty( "name" ) );
        index().index( andy, "title", andy.getProperty( "title" ) );
        index().index( larry, "name", larry.getProperty( "name" ) );
        index().index( larry, "title", larry.getProperty( "title" ) );
    
        // Correct Larry's name
        index().removeIndex( larry, "name",
            larry.getProperty( "name" ) );
        larry.setProperty( "name", "Larry Wachowski" );
        index().index( larry, "name",
            larry.getProperty( "name" ) );
    
        assertCollection( asCollection( index().getNodes(
            "name", "Andy AND Wachowski" ) ), andy );
        assertCollection( asCollection( index().getNodes(
            "name", "Larry AND Wachowski" ) ), larry );
    }
    
    @Override
    protected String dirName()
    {
        return "lucene-fulltext";
    }
}
