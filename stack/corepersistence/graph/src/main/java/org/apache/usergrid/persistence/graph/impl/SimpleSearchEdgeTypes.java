package org.apache.usergrid.persistence.graph.impl;


import org.apache.usergrid.persistence.collection.mvcc.entity.ValidationUtils;
import org.apache.usergrid.persistence.graph.SearchEdgeTypes;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.common.base.Optional;


/**
 *
 *
 */
public class SimpleSearchEdgeTypes implements SearchEdgeTypes {

    private final Id node;
    private final Optional<String> last;


    public SimpleSearchEdgeTypes( final Id node, final String last ) {
        ValidationUtils.verifyIdentity( node );
        this.node = node;
        this.last = Optional.fromNullable( last );
    }


    @Override
    public Id getNode() {
        return node;
    }


    @Override
    public Optional<String> getLast() {
        return last;
    }
}
