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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.neo4j.helpers.collection.PrefetchingIterator;

class DocToIdIterator extends PrefetchingIterator<Long>
{
    private final Iterator<Document> docs;
    private final Collection<Long> exclude;
    private final IndexSearcherRef searcherOrNull;
    private final Set<Long> alreadyReturnedIds = new HashSet<Long>();
    
    DocToIdIterator( Iterator<Document> docs, Collection<Long> exclude,
        IndexSearcherRef searcherOrNull )
    {
        this.docs = docs;
        this.exclude = exclude;
        this.searcherOrNull = searcherOrNull;
    }

    @Override
    protected Long fetchNextOrNull()
    {
        Long result = null;
        while ( result == null )
        {
            if ( !docs.hasNext() )
            {
                endReached();
                break;
            }
            Document doc = docs.next();
            long id = Long.parseLong(
                doc.getField( LuceneIndexService.DOC_ID_KEY ).stringValue() );
            if ( exclude == null || !exclude.contains( id ) )
            {
                if ( alreadyReturnedIds.add( id ) )
                {
                    result = id;
                }
            }
        }
        return result;
    }
    
    private void endReached()
    {
        if ( this.searcherOrNull != null )
        {
            this.searcherOrNull.closeStrict();
        }
    }

    public int size()
    {
        return ( ( HitsIterator ) this.docs ).size();
    }
}
