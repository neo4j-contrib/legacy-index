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

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;

abstract class GenericIndexService implements IndexService
{
    private final NeoService neo;
    private final IndexServiceQueue queue;
    
    private ThreadLocal<Isolation> threadIsolation =
        new ThreadLocal<Isolation>()
        {
            @Override
            protected Isolation initialValue()
            {
                return Isolation.SAME_TX;
            }
        };
    
    protected abstract void removeIndexThisTx( Node node, String key, 
        Object value );

    protected abstract void indexThisTx( Node node, String key, Object value );

    public GenericIndexService( NeoService service )
    {
        if ( service == null )
        {
            throw new IllegalArgumentException( "Null neo service" );
        }
        this.neo = service;
        queue = new IndexServiceQueue( this );
        queue.start();
    }
    
    public void index( Node node, String key, Object value )
    {
        Isolation level = threadIsolation.get();
        if ( level == Isolation.SAME_TX )
        {
            indexThisTx( node, key, value );
        }
        else
        {
            queue.queueIndex( level, node, key, value );
        }
    }
    
    public void removeIndex( Node node, String key, Object value )
    {
        Isolation level = threadIsolation.get();
        if ( level == Isolation.SAME_TX )
        {
            removeIndexThisTx( node, key, value );
        }
        else
        {
            queue.queueRemove( level, node, key, value );
        }
    }
    
    protected NeoService getNeo()
    {
        return neo;
    }
    
    public void setIsolation( Isolation level )
    {
        threadIsolation.set( level );
    }
    
    public Isolation getIsolation()
    {
        return threadIsolation.get();
    }
    
    protected Transaction beginTx()
    {
        return neo.beginTx();
    }
    
    public void shutdown()
    {
        queue.stopRunning();
    }
    
    protected IndexServiceQueue getQueue()
    {
        return queue;
    }
}
