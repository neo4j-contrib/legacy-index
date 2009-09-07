package org.neo4j.util.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.impl.batchinsert.BatchInserter;

public class LuceneFulltextIndexBatchInserter
    extends LuceneIndexBatchInserterImpl
{
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
