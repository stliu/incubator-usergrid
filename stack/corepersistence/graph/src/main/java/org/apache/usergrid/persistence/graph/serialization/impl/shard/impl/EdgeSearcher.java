package org.apache.usergrid.persistence.graph.serialization.impl.shard.impl;


import java.util.Iterator;

import org.apache.usergrid.persistence.core.astyanax.ColumnParser;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardEntries;

import com.google.common.base.Optional;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.util.RangeBuilder;


/**
 * Searcher to be used when performing the search.  Performs I/O transformation as well as parsing for the iterator. If
 * there are more row keys available to seek, the iterator will return true
 *
 * @param <R> The row type
 * @param <C> The column type
 * @param <T> The parsed return type
 */
public abstract class EdgeSearcher<R, C, T> implements ColumnParser<C, T>, Iterator<ScopedRowKey<ApplicationScope, R>> {

    protected final Optional<Edge> last;
    protected final long maxTimestamp;
    protected final ApplicationScope scope;
    protected final Iterator<ShardEntries> shards;


    protected EdgeSearcher( final ApplicationScope scope, final long maxTimestamp, final Optional<Edge> last,
                            final Iterator<ShardEntries> shards ) {
        this.scope = scope;
        this.maxTimestamp = maxTimestamp;
        this.last = last;
        this.shards = shards;
    }


    @Override
    public boolean hasNext() {
        return shards.hasNext();
    }


    @Override
    public ScopedRowKey<ApplicationScope, R> next() {
        /**
         * Todo, multi scan
         */
        return ScopedRowKey
                .fromKey( scope, generateRowKey( shards.next().getEntries().iterator().next().getShardIndex() ) );
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException( "Remove is unsupported" );
    }


    /**
     * Set the range on a search
     */
    public void setRange( final RangeBuilder builder ) {

        //set our start range since it was supplied to us
        if ( last.isPresent() ) {
            C sourceEdge = getStartColumn( last.get() );


            builder.setStart( sourceEdge, getSerializer() );
        }
        else {


        }
    }


    public boolean hasPage() {
        return last.isPresent();
    }


    @Override
    public T parseColumn( final Column<C> column ) {
        final C edge = column.getName();

        return createEdge( edge, column.getBooleanValue() );
    }


    /**
     * Get the column's serializer
     */
    protected abstract Serializer<C> getSerializer();


    /**
     * Create a row key for this search to use
     *
     * @param shard The shard to use in the row key
     */
    protected abstract R generateRowKey( final long shard );


    /**
     * Set the start column to begin searching from.  The last is provided
     */
    protected abstract C getStartColumn( final Edge last );


    /**
     * Create an edge to return to the user based on the directed edge provided
     *
     * @param column The column name
     * @param marked The marked flag in the column value
     */
    protected abstract T createEdge( final C column, final boolean marked );
}
