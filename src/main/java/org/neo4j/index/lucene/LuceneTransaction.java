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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.neo4j.graphdb.Node;
import org.neo4j.index.lucene.LuceneCommand.AddCommand;
import org.neo4j.index.lucene.LuceneCommand.RemoveCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;

class LuceneTransaction extends XaTransaction
{
    private final Map<String,Map<Object,Set<Long>>> txIndexed = 
        new HashMap<String,Map<Object,Set<Long>>>();
    private final Map<String,Map<Object,Set<Long>>> txRemoved = 
        new HashMap<String,Map<Object,Set<Long>>>();

    private final LuceneDataSource luceneDs;

    private final Map<String,List<LuceneCommand>> commandMap = 
        new HashMap<String,List<LuceneCommand>>();

    LuceneTransaction( int identifier, XaLogicalLog xaLog,
        LuceneDataSource luceneDs )
    {
        super( identifier, xaLog );
        this.luceneDs = luceneDs;
    }

    void index( Node node, String key, Object value )
    {
        insert( node, key, value, txRemoved, txIndexed );
    }

    void removeIndex( Node node, String key, Object value )
    {
        insert( node, key, value, txIndexed, txRemoved );
    }

    private void insert( Node node, String key, Object value,
        Map<String,Map<Object,Set<Long>>> toRemoveFrom,
        Map<String,Map<Object,Set<Long>>> toInsertInto )
    {
        delFromIndex( node, key, value, toRemoveFrom );
        Map<Object,Set<Long>> keyIndex = toInsertInto.get( key );
        if ( keyIndex == null )
        {
            keyIndex = new HashMap<Object,Set<Long>>();
            toInsertInto.put( key, keyIndex );
        }
        Set<Long> nodeIds = keyIndex.get( value );
        if ( nodeIds == null )
        {
            nodeIds = new HashSet<Long>();
        }
        nodeIds.add( node.getId() );
        keyIndex.put( value, nodeIds );
    }

    private boolean delFromIndex( Node node, String key, Object value,
        Map<String,Map<Object,Set<Long>>> map )
    {
        Map<Object,Set<Long>> keyIndex = map.get( key );
        if ( keyIndex == null )
        {
            return false;
        }
        Set<Long> nodeIds = keyIndex.get( value );
        if ( nodeIds != null )
        {
            return nodeIds.remove( node.getId() );
        }
        return false;
    }

    Set<Long> getDeletedNodesFor( String key, Object value )
    {
        Map<Object,Set<Long>> keyIndex = txRemoved.get( key );
        if ( keyIndex != null )
        {
            Set<Long> nodeIds = keyIndex.get( value );
            if ( nodeIds != null )
            {
                return nodeIds;
            }
        }
        return Collections.emptySet();
    }

    Set<Long> getNodesFor( String key, Object value )
    {
        Map<Object,Set<Long>> keyIndex = txIndexed.get( key );
        if ( keyIndex != null )
        {
            Set<Long> nodeIds = keyIndex.get( value );
            if ( nodeIds != null )
            {
                return nodeIds;
            }
        }
        return Collections.emptySet();
    }
    
    protected LuceneDataSource getDataSource()
    {
        return this.luceneDs;
    }
    
    private void indexWriter( IndexWriter writer, long nodeId, String key,
        Object value )
    {
        Document document = new Document();
        this.luceneDs.fillDocument( document, nodeId, key, value );
        try
        {
            writer.addDocument( document );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected void doAddCommand( XaCommand command )
    {
        LuceneCommand luceneCommand = ( LuceneCommand ) command;
        String key = luceneCommand.getKey();
        List<LuceneCommand> list = commandMap.get( key );
        if ( list == null )
        {
            list = new ArrayList<LuceneCommand>();
            commandMap.put( key, list );
        }
        list.add( luceneCommand );
    }

    @Override
    protected void doCommit()
    {
        luceneDs.getWriteLock();
        try
        {
            for ( Map.Entry<String, List<LuceneCommand>> entry :
                this.commandMap.entrySet() )
            {
                String key = entry.getKey();
                IndexWriter writer = luceneDs.getIndexWriter( key );
                for ( LuceneCommand command : entry.getValue() )
                {
                    if ( command instanceof AddCommand )
                    {
                        indexWriter( writer, command.getNodeId(), key,
                            command.getValue() );
                    }
                    else if ( command instanceof RemoveCommand )
                    {
                        luceneDs.deleteDocumentsUsingWriter(
                            writer, command.getNodeId(), command.getValue() );
                    }
                    else
                    {
                        throw new RuntimeException( "Unknown command type " +
                            command + ", " + command.getClass() );
                    }
                    luceneDs.invalidateCache( key, command.getValue() );
                }
                luceneDs.removeWriter( key, writer );
                luceneDs.invalidateIndexSearcher( key );
            }
        }
        finally
        {
            luceneDs.releaseWriteLock();
        }
    }

    @Override
    protected void doPrepare()
    {
        for ( String key : txIndexed.keySet() )
        {
            Map<Object,Set<Long>> addIndex = txIndexed.get( key );
            for ( Object object : addIndex.keySet() )
            {
                for ( long id : addIndex.get( object ) )
                {
                    addCommand( new AddCommand( id, key, object.toString() ) );
                }
            }
        }
        for ( String key : txRemoved.keySet() )
        {
            Map<Object,Set<Long>> removeIndex = txRemoved.get( key );
            for ( Object object : removeIndex.keySet() )
            {
                for ( long id : removeIndex.get( object ) )
                {
                    addCommand( new RemoveCommand( id, key, 
                        object.toString() ) );
                }
            }
        }
    }

    @Override
    protected void doRollback()
    {
        // TODO Auto-generated method stub
        commandMap.clear();
        txIndexed.clear();
        txRemoved.clear();
    }

    @Override
    public boolean isReadOnly()
    {
        return false;
    }
}