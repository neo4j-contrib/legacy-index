/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index;

/**
 * Thrown in a read-only index when the user tries to use for example the
 * {@link IndexService#index(org.neo4j.graphdb.Node, String, Object)} or
 * {@link IndexService#removeIndex(org.neo4j.graphdb.Node, String, Object)}
 * methods.
 */
public class ReadOnlyIndexException extends RuntimeException
{
    /**
     * Constructs an exception with a default message.
     */
    public ReadOnlyIndexException()
    {
        super( "You cannot modify a read-only index" );
    }
}
