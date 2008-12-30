package org.neo4j.util.index;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.api.core.NeoService;

public class LuceneFulltextIndexService extends LuceneIndexService
{
    protected static final String DOC_INDEX_SOURCE_KEY = "index_source";
    
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
        return super.getDirName() + "-fulltext";
    }

    @Override
    protected byte[] getXaResourceId()
    {
        return "262374".getBytes();
    }

    @Override
    protected Query formQuery( Object value )
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
