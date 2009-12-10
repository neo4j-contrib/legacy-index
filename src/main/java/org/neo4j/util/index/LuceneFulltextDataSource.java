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

import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.neo4j.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.impl.transaction.xaframework.XaTransaction;

/**
 * A lucene XA data store for storing fulltext indexing.
 */
public class LuceneFulltextDataSource extends LuceneDataSource
{
    /**
     * Constructs a {@link LuceneFulltextDataSource}.
     * 
     * @param params XA parameters
     * @throws InstantiationException if the data source couldn't be
     * instantiated
     */
    public LuceneFulltextDataSource( Map<Object, Object> params )
        throws InstantiationException
    {
        super( params );
    }

    @Override
    public XaTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog )
    {
        return new LuceneFulltextTransaction( identifier, logicalLog, this );
    }

    @Override
    protected Index getIndexStrategy( String key, Object value )
    {
        return Index.ANALYZED;
    }

    @Override
    protected String getDeleteDocumentsKey()
    {
        return LuceneFulltextIndexService.DOC_INDEX_SOURCE_KEY;
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
}
