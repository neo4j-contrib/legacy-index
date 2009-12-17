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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.neo4j.api.core.NeoService;

/**
 * A {@link LuceneFulltextIndexService} which treats the value in
 * {@link #getNodes(String, Object)} as a Lucene query, given in the
 * Lucene query syntax.
 * 
 * See more information at http://wiki.neo4j.org/content/Indexing_with_IndexService#A_great_subclass_to_LuceneFulltextIndexService
 */
public class LuceneFulltextQueryIndexService extends LuceneFulltextIndexService
{
    private static final Analyzer WHITESPACE_ANALYZER =
        new WhitespaceAnalyzer();
    
    /**
     * @param neo the {@link NeoService} to use.
     */
    public LuceneFulltextQueryIndexService( NeoService neo )
    {
        super( neo );
    }

    @Override
    protected Query formQuery( String key, Object value )
    {
        try
        {
           return new QueryParser( Version.LUCENE_CURRENT, DOC_INDEX_KEY,
              WHITESPACE_ANALYZER ).parse( value.toString() );
        }
        catch ( ParseException e )
        {
           throw new RuntimeException( e );
        }
    }
}
