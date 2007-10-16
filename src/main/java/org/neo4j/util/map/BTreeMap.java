package org.neo4j.util.map;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.btree.BTree;
import org.neo4j.util.btree.KeyEntry;

/**
 * A map implementation using {@link org.neo4j.util.btree.BTree BTree}
 * <p>
 * Note: this implementation is not thread safe (yet).
 */
// not thread safe yet
public class BTreeMap
{
	public static enum RelTypes implements RelationshipType
	{
		MAP_ENTRY,
	}
	
	private static final Object GOTO_NODE = Long.MIN_VALUE;
	
	private static final String MAP_NAME = "index_name";
	private static final String MAP_KEY = "map_key";
	private static final String MAP_VALUE = "map_value";
	// private static final String GOTO_NODE = "goto_node";
	
	private final Node underlyingNode;
	private BTree bTree;
	private String name;
	private EmbeddedNeo neo;
	
	
	/**
	 * Creates/loads a persistent map based on a b-tree. 
	 * The <CODE>underlyingNode</CODE> can either be a new (just created) node 
	 * or a node that already represents a previously created map.
	 *
	 * @param name The unique name of the map or null if map already
	 * created (using specified underlying node)
	 * @param underlyingNode The underlying node representing the map
	 * @param neo The embedded neo instance
	 * @throws IllegalArgumentException if the underlying node is a map with
	 * a different name set.
	 */
	public BTreeMap( String name, Node underlyingNode, EmbeddedNeo neo )
	{
		if ( underlyingNode == null || neo == null )
		{
			throw new IllegalArgumentException( 
				"Null parameter underlyingNode=" + underlyingNode +
				" neo=" + neo );
		}
		this.underlyingNode = underlyingNode;
		this.neo = neo;
		this.neo.registerEnumRelationshipTypes( RelTypes.class );
		this.neo.registerEnumRelationshipTypes( 
			org.neo4j.util.btree.BTree.RelTypes.class );
		Transaction tx = Transaction.begin();
		try
		{
			if ( underlyingNode.hasProperty( MAP_NAME ) )
			{
				String storedName = (String) underlyingNode.getProperty( 
					MAP_NAME );
				if ( name != null && !storedName.equals( name ) )
				{
					throw new IllegalArgumentException( "Name of map " + 
						"for node=" + underlyingNode.getId() + "," + 
						storedName + " is not same as passed in name=" + 
						name );
				}
				if ( name == null )
				{
					this.name = (String) underlyingNode.getProperty( 
						MAP_NAME );
				}
				else
				{
					this.name = name;
				}
			}
			else
			{
				underlyingNode.setProperty( MAP_NAME, name );
				this.name = name;
			}
			Relationship bTreeRel = underlyingNode.getSingleRelationship( 
				org.neo4j.util.btree.BTree.RelTypes.TREE_ROOT, 
				Direction.OUTGOING );
			if ( bTreeRel != null )
			{
				bTree = new BTree( neo, bTreeRel.getEndNode() );
			}
			else
			{
				Node bTreeNode = neo.createNode();
				underlyingNode.createRelationshipTo( bTreeNode, 
					org.neo4j.util.btree.BTree.RelTypes.TREE_ROOT );
				bTree = new BTree( neo, bTreeNode );
			}
			tx.success();
		}
		finally
		{
			tx.finish();
		}
	}
	
	public String getName()
	{
		return this.name;
	}
	
	Node getUnderlyingNode()
	{
		return underlyingNode;
	}
	
	/**
	 * If key or value is <CODE>null</CODE> {@link IllegalArgumentException} is 
	 * thrown. Key and value must be valid neo properties.
	 */
	public void put( Object key, Object value )
	{
		if ( key == null || value == null )
		{
			throw new IllegalArgumentException( "Null node" );
		}
		Transaction tx = Transaction.begin();
		try
		{
			int hashCode = key.hashCode();
			KeyEntry entry = bTree.addIfAbsent( hashCode, value );
			if ( entry != null )
			{
				entry.setKeyValue( key );
			}
			else
			{
				entry = bTree.getAsKeyEntry( hashCode );
				Object goOtherNode = entry.getKeyValue();
				Node bucketNode = null;
				if ( !goOtherNode.equals( GOTO_NODE ) )
				{
					Object prevValue = entry.getValue();
					Object prevKey = entry.getKeyValue();
					if ( prevKey.equals( key ) )
					{
						entry.setValue( value );
						tx.success();
						return;
					}
					entry.setKeyValue( GOTO_NODE );
					bucketNode = neo.createNode();
					entry.setValue( bucketNode.getId() );
					Node prevEntry = neo.createNode();
					bucketNode.createRelationshipTo( prevEntry, 
						RelTypes.MAP_ENTRY );
					prevEntry.setProperty( MAP_KEY, prevKey );
					prevEntry.setProperty( MAP_VALUE, prevValue );
					Node newEntry = neo.createNode();
					bucketNode.createRelationshipTo( newEntry, 
						RelTypes.MAP_ENTRY );
					newEntry.setProperty( MAP_KEY, key );
					newEntry.setProperty( MAP_VALUE, value );
				}
				else
				{
					bucketNode = neo.getNodeById( (Long) entry.getValue() );
					for ( Relationship rel : bucketNode.getRelationships( 
						RelTypes.MAP_ENTRY, Direction.OUTGOING ) )
					{
						Node entryNode = rel.getEndNode();
						if ( entryNode.getProperty( MAP_KEY ).equals( key ) )
						{
							entryNode.setProperty( MAP_VALUE, value );
							tx.success();
							return;
						}
					}
					Node newEntry = neo.createNode();
					bucketNode.createRelationshipTo( newEntry, 
						RelTypes.MAP_ENTRY );
					newEntry.setProperty( MAP_KEY, key );
					newEntry.setProperty( MAP_VALUE, value );
				}
			}
			tx.success();
		}
		finally
		{
			tx.finish();
		}
	}
	
	public Object remove( Object key )
	{
		Transaction tx = Transaction.begin();
		try
		{
			int hashCode = key.hashCode();
			KeyEntry entry = bTree.getAsKeyEntry( hashCode );
			if ( entry != null )
			{
				Object goOtherNode = entry.getKeyValue();
				if ( !goOtherNode.equals( GOTO_NODE ) )
				{
					if ( goOtherNode.equals( key ) )
					{
						Object value = entry.getValue();
						entry.remove();
						tx.success();
						return value;
					}
				}
				else
				{
					Node bucketNode = neo.getNodeById( 
						(Long) entry.getValue() );
					for ( Relationship rel : bucketNode.getRelationships( 
						RelTypes.MAP_ENTRY, Direction.OUTGOING ) )
					{
						Node entryNode = rel.getEndNode();
						if ( entryNode.getProperty( MAP_KEY ).equals( key ) )
						{
							Object value = entryNode.getProperty( MAP_VALUE );
							rel.delete();
							entryNode.delete();
							tx.success();
							return value;
						}
					}
				}
			}
			tx.success();
			return null;
		}
		finally
		{
			tx.finish();
		}
	}
	
	/**
	 * Public only for testing purposes. Validates the interal b-tree index and 
	 * if any error is found a runtime exception is thrown.
	 */
	public void validate()
	{
		bTree.validateTree();
	}
	
	public Object get( Object key )
	{
		Transaction tx = Transaction.begin();
		try
		{
			int hashCode = key.hashCode();
			KeyEntry entry = bTree.getAsKeyEntry( hashCode );
			if ( entry != null )
			{
				Object goOtherNode = entry.getKeyValue();
				if ( !goOtherNode.equals( GOTO_NODE ) )
				{
					if ( goOtherNode.equals( key ) )
					{
						tx.success();
						return entry.getValue();
					}
				}
				else
				{
					Node bucketNode = neo.getNodeById( 
						(Long) entry.getValue() );
					for ( Relationship rel : bucketNode.getRelationships( 
						RelTypes.MAP_ENTRY, Direction.OUTGOING ) )
					{
						Node entryNode = rel.getEndNode();
						if ( entryNode.getProperty( MAP_KEY ).equals( key ) )
						{
							tx.success();
							return entryNode.getProperty( MAP_VALUE );
						}
					}
				}
			}
			tx.success();
			return null;
		}
		finally
		{
			tx.finish();
		}
	}
	
	public void clear()
	{
	    throw new UnsupportedOperationException();
	}

	public Iterable<Object> values()
    {
	    throw new UnsupportedOperationException();
    }

	public Iterable<Object> keySet()
    {
	    throw new UnsupportedOperationException();
    }
}
