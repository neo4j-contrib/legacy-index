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
	 * Returns a single node indexed with <CODE>indexKey</CODE>. If more 
	 * then one node is indexed with that key a <CODE>RuntimeException</CODE>
	 * is thrown. If no node is indexed with the key <CODE>null</CODE> is 
	 * returned.
	 * 
	 * @param indexKey the index key
	 * @return the single node mapped to <CODE>indexKey</CODE>
	 */
	public Node getSingleNodeFor( Object indexKey );
	
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
    
    public void clear();
    
	/**
	 * Returns all nodes in this index. Same node may be returned many times 
	 * depending on implementation.
	 * 
	 * @return all nodes in this index
	 * @throws UnsupportedOperationException if the <CODE>values()</CODE>
	 * method isn't supported by this index.
	 */
	public Iterable<Node> values();
}
