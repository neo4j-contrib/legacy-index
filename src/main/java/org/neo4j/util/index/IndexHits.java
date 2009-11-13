package org.neo4j.util.index;

import org.neo4j.api.core.Node;

public interface IndexHits extends Iterable<Node>
{
    int size();
}
