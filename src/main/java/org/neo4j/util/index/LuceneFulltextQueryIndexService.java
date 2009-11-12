package org.neo4j.util.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.neo4j.api.core.NeoService;

public class LuceneFulltextQueryIndexService extends LuceneFulltextIndexService
{
    private static final Analyzer WHITESPACE_ANALYZER =
        new WhitespaceAnalyzer();
    
    public LuceneFulltextQueryIndexService( NeoService neo )
    {
        super( neo );
    }

    @Override
    protected Query formQuery( String key, Object value )
    {
        try
        {
           return new QueryParser( Version.LUCENE_CURRENT, DOC_INDEX_KEY,
              WHITESPACE_ANALYZER ).parse( value.toString() );
        }
        catch ( ParseException e )
        {
           throw new RuntimeException( e );
        }
    }
}
