/*
 * Copyright (c) 2010 "Neo Technology,"
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

/**
 * Provides indexing capabilities to the Neo4j graph. This component is a
 * collection of various utilities for indexing parts of a Neo4j graph in 
 * different ways. The most straight-forward way is via
 * {@link org.neo4j.index.IndexService}. It is basically a service where you can
 * index Neo4j nodes with key-value pairs.
 */
package org.neo4j.index;

