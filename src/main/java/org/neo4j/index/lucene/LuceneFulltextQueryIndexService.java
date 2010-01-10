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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;

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
     * @param graphDb the {@link GraphDatabaseService} to use.
     */
    public LuceneFulltextQueryIndexService( GraphDatabaseService graphDb )
    {
        super( graphDb );
    }

    @Override
    protected Query formQuery( String key, Object value )
    {
        try
        {
            QueryParser parser = new QueryParser( Version.LUCENE_CURRENT,
                DOC_INDEX_KEY,
                    LuceneDataSource.LOWER_CASE_WHITESPACE_ANALYZER );
            Operator operator = getDefaultQueryOperator( key, value );
            if ( operator != null )
            {
                parser.setDefaultOperator( operator );
            }
            return parser.parse( value.toString() );
        }
        catch ( ParseException e )
        {
           throw new RuntimeException( e );
        }
    }

    /**
     * Returns the default operator (AND or OR) used when parsing the query.
     * See more information at http://lucene.apache.org
     * 
     * @param key the index key.
     * @param value the lucene query.
     * @return the default operator (AND or OR ) used when parsing the query.
     */
    public Operator getDefaultQueryOperator( String key, Object value )
    {
        return null;
    }

    /**
     * Here the {@code value} is treated as a lucene query,
     * http://lucene.apache.org/java/2_9_1/queryparsersyntax.html
     * 
     * So if you've indexed node (1) with value "Andy Wachowski" and node (2)
     * with "Larry Wachowski" you can expect this behaviour if you query for:
     * 
     * <ul>
     * <li>"andy" --> (1)</li>
     * <li>"Andy" --> (1)</li>
     * <li>"wachowski" --> (1), (2)</li>
     * <li>"+wachow* +larry" --> (2)</li>
     * <li>"andy AND larry" --></li> 
     * <li>"andy OR larry" --> (1), (2)</li>
     * <li>"larry Wachowski" --> (1), (2)</li>
     * </ul>
     * 
     * The default AND/OR behaviour can be changed by overriding
     * {@link #getDefaultQueryOperator(String, Object)}.
     */
    @Override
    public IndexHits<Node> getNodes( String key, Object value )
    {
        return super.getNodes( key, value );
    }
}
