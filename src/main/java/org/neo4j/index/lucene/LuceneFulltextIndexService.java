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

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;

/**
 * A {@link LuceneIndexService} which indexes the values with fulltext indexing.
 * Fulltext means that the indexing process takes the values you throw in and
 * tokenizes those into words so that you can query for those individual words
 * in {@link #getNodes(String, Object)}. Also queries are case-insensitive.
 * 
 * It stores more data per Lucene entry to make this possible. This makes it
 * incompatible with {@link LuceneIndexService} so it has got its own XA
 * resource ID. This means that you can have one {@link LuceneIndexService} and
 * one {@link LuceneFulltextIndexService} for a {@link GraphDatabaseService}.
 * 
 * See more information at
 * http://wiki.neo4j.org/content/Indexing_with_IndexService#Fulltext_indexing
 */
public class LuceneFulltextIndexService extends LuceneIndexService
{
    protected static final String DOC_INDEX_SOURCE_KEY = "index_source";
    protected static final String FULLTEXT_DIR_NAME_POSTFIX = "-fulltext";

    /**
     * @param graphDb the {@link GraphDatabaseService} to use.
     */
    public LuceneFulltextIndexService( GraphDatabaseService graphDb )
    {
        super( graphDb );
    }

    @Override
    protected Class<? extends LuceneDataSource> getDataSourceClass()
    {
        return LuceneFulltextDataSource.class;
    }

    @Override
    protected String getDirName()
    {
        return super.getDirName() + FULLTEXT_DIR_NAME_POSTFIX;
    }

    @Override
    protected byte[] getXaResourceId()
    {
        return "262374".getBytes();
    }

    /**
     * Since this is a "fulltext" index it changes the contract of this method
     * slightly. It treats the {@code value} more like a query in than you can
     * query for individual words in your indexed values.
     * 
     * So if you've indexed node (1) with value "Andy Wachowski" and node (2)
     * with "Larry Wachowski" you can expect this behaviour if you query for:
     * 
     * <ul>
     * <li>"addy" --> (1)</li>
     * <li>"Andy" --> (1)</li>
     * <li>"wachowski" --> (1), (2)</li>
     * <li>"andy larry" --></li>
     * <li>"larry Wachowski" --> (2)</li>
     * <li>"wachowski Andy" --> (1)</li>
     * </ul>
     */
    @Override
    public IndexHits<Node> getNodes( String key, Object value )
    {
        return super.getNodes( key, value );
    }

    @Override
    protected Query formQuery( String key, Object value )
    {
        TokenStream stream = LuceneFulltextDataSource.LOWER_CASE_WHITESPACE_ANALYZER.tokenStream(
                DOC_INDEX_KEY,
                new StringReader( value.toString().toLowerCase() ) );
        Token token = new Token();
        BooleanQuery booleanQuery = new BooleanQuery();
        try
        {
            while ( ( token = stream.next( token ) ) != null )
            {
                String term = token.term();
                booleanQuery.add(
                        new TermQuery( new Term( DOC_INDEX_KEY, term ) ),
                        Occur.MUST );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return booleanQuery;
    }

    @Override
    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        // For now, or is it just not feasable
        throw new UnsupportedOperationException();
    }
}
