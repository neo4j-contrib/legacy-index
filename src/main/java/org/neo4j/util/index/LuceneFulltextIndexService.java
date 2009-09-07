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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.api.core.NeoService;

public class LuceneFulltextIndexService extends LuceneIndexService
{
    protected static final String DOC_INDEX_SOURCE_KEY = "index_source";
    protected static final String FULLTEXT_DIR_NAME_POSTFIX = "-fulltext";
    
    public LuceneFulltextIndexService( NeoService neo )
    {
        super( neo );
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

    @Override
    protected Query formQuery( String key, Object value )
    {
        return new TermQuery( new Term( DOC_INDEX_KEY,
            value.toString().toLowerCase() ) );
    }

    @Override
    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        // For now, or is it just not feasable
        throw new UnsupportedOperationException();
    }
}
