package org.neo4j.util.index;

import org.neo4j.api.core.Node;

/**
 * A index that indexes nodes with a key. 
 * 
 * @see SimpleIndex, MultiIndex
 */
public interface Index
{
	/**
	 * Create a index mapping between a node and a key.
	 * 
	 * @param nodeToIndex the node to index
	 * @param indexKey the key
	 */
	public void index( Node nodeToIndex, Object indexKey );
	
	/**
	 * Returns nodes indexed with <CODE>indexKey</CODE>
	 * 
	 * @param indexKey the index key
	 * @return nodes mapped to <CODE>indexKey</CODE>
	 */
	public Iterable<Node> getNodesFor( Object indexKey );
	
	/**
	 * Removes a index mapping between a node and a key.
	 * 
	 * @param nodeToRemove node to remove
	 * @param indexKey the key
	 */
	public void remove( Node nodeToRemove, Object indexKey );
	
	/**
	 * Deletes this index.
	 */
	public void drop();
	
	/**
	 * Deletes this index using a commit interval.
	 * 
	 * @param commitInterval number of index mappings removed before a new 
	 * transaction is started
	 */
	public void drop( int commitInterval );
	
	/**
	 * Returns all nodes in this index. Same node may be returned many times 
	 * depending on implementation.
	 * 
	 * @return all nodes in this index
	 */
	public Iterable<Node> values();
}
