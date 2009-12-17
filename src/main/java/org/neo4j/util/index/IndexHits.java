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
package org.neo4j.util.index;

/**
 * It's just an Iterable<T> which has a {@link #size()} method on it.
 * Ideally the size is calculated in some other (more optimized) way than
 * looping through all the items in the iterator so it's ok using any way you
 * like.
 * 
 * @param <T> the type of items in the Iterable.
 */
public interface IndexHits<T> extends Iterable<T>
{
    /**
     * Returns the size of this iterable. Ideally the size is given at
     * construction time so that the size is known before-hand. This method
     * should _not_ be implemented as looping through all the items.
     * 
     * @return the size if this iterable.
     */
    int size();
}
