package org.neo4j.util.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.neo4j.api.core.NeoService;
import org.neo4j.impl.transaction.xaframework.XaLogicalLog;

public class LuceneFulltextIndexService extends LuceneIndexService
{
    private static final String DOC_INDEX_SOURCE_KEY = "index_source";
    
    public LuceneFulltextIndexService( NeoService neo )
    {
        super( neo );
    }

    @Override
    protected byte[] getXaResourceId()
    {
        return "162374".getBytes();
    }

    @Override
    protected Index getIndexStrategy()
    {
        return Field.Index.ANALYZED;
    }

    @Override
    protected void fillDocument( Document document, long nodeId, Object value )
    {
        super.fillDocument( document, nodeId, value );
        document.add( new Field( DOC_INDEX_SOURCE_KEY, value.toString(),
            Field.Store.NO, Field.Index.NOT_ANALYZED ) );
    }
    
    @Override
    protected String getDeleteDocumentsKey()
    {
        return DOC_INDEX_SOURCE_KEY;
    }

    @Override
    public void enableCache( String key, int maxNumberOfCachedEntries )
    {
        // For now, or is it just not feasable
        throw new UnsupportedOperationException();
    }

    @Override
    protected LuceneTransaction createTransaction( int identifier,
        XaLogicalLog logicalLog, LuceneDataSource dataSource )
    {
        return new LuceneFulltextTransaction(
            identifier, logicalLog, dataSource );
    }
}
