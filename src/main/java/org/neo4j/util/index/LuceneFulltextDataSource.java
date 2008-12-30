package org.neo4j.util.index;

import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.neo4j.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.impl.transaction.xaframework.XaTransaction;

public class LuceneFulltextDataSource extends LuceneDataSource
{
    public LuceneFulltextDataSource( Map<?, ?> params )
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
    protected Index getIndexStrategy()
    {
        return Index.ANALYZED;
    }

    @Override
    protected String getDeleteDocumentsKey()
    {
        return LuceneFulltextIndexService.DOC_INDEX_SOURCE_KEY;
    }
    
    @Override
    protected void fillDocument( Document document, long nodeId, Object value )
    {
        super.fillDocument( document, nodeId, value );
        document.add( new Field(
            LuceneFulltextIndexService.DOC_INDEX_SOURCE_KEY, value.toString(),
            Field.Store.NO, Field.Index.NOT_ANALYZED ) );
    }
}
