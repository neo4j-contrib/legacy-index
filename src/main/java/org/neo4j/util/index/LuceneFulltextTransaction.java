package org.neo4j.util.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.neo4j.api.core.Node;
import org.neo4j.impl.transaction.xaframework.XaLogicalLog;

public class LuceneFulltextTransaction extends LuceneTransaction
{
    private final Map<String, Directory> fulltextIndexed =
        new HashMap<String, Directory>();
    private final Map<String, Directory> fulltextRemoved =
        new HashMap<String, Directory>();
    
    LuceneFulltextTransaction( int identifier, XaLogicalLog xaLog,
        LuceneDataSource luceneDs )
    {
        super( identifier, xaLog, luceneDs );
    }
    
    private Directory getDirectory( Map<String, Directory> map, String key )
    {
        Directory directory = map.get( key );
        if ( directory == null )
        {
            directory = new RAMDirectory();
            try
            {
                IndexWriter writer = new IndexWriter( directory,
                    getDataSource().getIndexService().getAnalyzer(), true,
                    MaxFieldLength.UNLIMITED );
                writer.close();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            map.put( key, directory );
        }
        return directory;
    }
    
    private IndexWriter newIndexWriter( Directory directory )
        throws IOException
    {
        return new IndexWriter( directory,
            getDataSource().getIndexService().getAnalyzer(),
            MaxFieldLength.UNLIMITED );
    }
    
    private void insertAndRemove( Directory insertTo, Directory removeFrom,
        Node node, String key, Object value )
    {
        try
        {
            Document document = new Document();
            this.getDataSource().fillDocument( document, node.getId(), value );
            IndexWriter writer = newIndexWriter( insertTo );
            writer.addDocument( document );
            writer.close();
            
            writer = newIndexWriter( removeFrom );
            writer.deleteDocuments( new Term( getDataSource().getIndexService().
                getDeleteDocumentsKey(), value.toString() ) );
            writer.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    void index( Node node, String key, Object value )
    {
        super.index( node, key, value );
        insertAndRemove( getDirectory( fulltextIndexed, key ),
            getDirectory( fulltextRemoved, key ), node, key, value );
    }

    @Override
    void removeIndex( Node node, String key, Object value )
    {
        super.removeIndex( node, key, value );
        insertAndRemove( getDirectory( fulltextRemoved, key ),
            getDirectory( fulltextIndexed, key ), node, key, value );
    }
    
    @Override
    Set<Long> getDeletedNodesFor( String key, Object value )
    {
        return getNodes( getDirectory( fulltextRemoved, key ), key,
            value );
    }

    @Override
    Set<Long> getNodesFor( String key, Object value )
    {
        return getNodes( getDirectory( fulltextIndexed, key ), key,
            value );
    }
    
    private Set<Long> getNodes( Directory directory, String key,
        Object value )
    {
        try
        {
            IndexSearcher searcher = new IndexSearcher( directory );
            Hits hits = searcher.search( new TermQuery(
                new Term( LuceneIndexService.DOC_INDEX_KEY,
                    value.toString() ) ) );
            HashSet<Long> result = new HashSet<Long>();
            for ( int i = 0; i < hits.length(); i++ )
            {
                result.add( Long.parseLong( hits.doc( i ).getField(
                    LuceneIndexService.DOC_ID_KEY ).stringValue() ) );
            }
            searcher.close();
            return result;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
