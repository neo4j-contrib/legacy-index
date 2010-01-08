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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.impl.batchinsert.BatchInserter;

/**
 * The "batch inserter" version of {@link LuceneFulltextIndexService}.
 * It should be used with a BatchInserter and stores the indexes in the same
 * format as {@link LuceneFulltextIndexService}.
 * 
 * It's optimized for large chunks of either reads or writes. So try to avoid
 * mixed reads and writes because there's a slight overhead to go from read mode
 * to write mode (the "mode" is per key and will not affect other keys)
 * 
 * See more information at http://wiki.neo4j.org/content/Indexing_with_BatchInserter
 */
public class LuceneFulltextIndexBatchInserter
    extends LuceneIndexBatchInserterImpl
{
    /**
     * @param neo the {@link BatchInserter} to use.
     */
    public LuceneFulltextIndexBatchInserter( BatchInserter neo )
    {
        super( neo );
    }

    @Override
    protected void fillDocument( Document document, long nodeId, String key,
        Object value )
    {
        super.fillDocument( document, nodeId, key, value );
        document.add( new Field(
            LuceneFulltextIndexService.DOC_INDEX_SOURCE_KEY, value.toString(),
            Field.Store.NO, Field.Index.NOT_ANALYZED ) );
    }

    @Override
    protected Index getIndexStrategy()
    {
        return Field.Index.ANALYZED;
    }

    @Override
    protected String getDirName()
    {
        return super.getDirName() +
            LuceneFulltextIndexService.FULLTEXT_DIR_NAME_POSTFIX;
    }

    @Override
    protected Query formQuery( String key, Object value )
    {
        return new TermQuery( new Term( LuceneIndexService.DOC_INDEX_KEY,
            value.toString().toLowerCase() ) );
    }
}
