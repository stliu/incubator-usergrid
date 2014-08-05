package org.apache.usergrid.persistence.graph.serialization.impl.shard.impl;


import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.collections4.iterators.PushbackIterator;

import org.apache.usergrid.persistence.graph.serialization.impl.shard.Shard;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardEntryGroup;


/**
 * Utility class that will take an iterator of all shards, and combine them into an iterator
 * of ShardEntryGroups.  These groups can then be used in a distributed system to handle concurrent reads and writes
 */
public class ShardEntryGroupIterator implements Iterator<ShardEntryGroup> {


    private ShardEntryGroup next;
    private final PushbackIterator<Shard> sourceIterator;
    private final long minDelta;


    /**
     * Create a shard iterator
     * @param shardIterator The iterator of all shards.  Order is expected to be by the  shard index from Long.MAX to Long.MIN
     * @param minDelta The minimum delta we allow to consider shards the same group
     */
    public ShardEntryGroupIterator( final Iterator<Shard> shardIterator, final long minDelta ) {
        this.sourceIterator = new PushbackIterator( shardIterator );

        /**
         * If we don't have any shards, we need to push our "MIN" shard into the list
         */
        if(!sourceIterator.hasNext()){
            sourceIterator.pushback( new Shard(0, 0, true) );
        }

        this.minDelta = minDelta;
    }


    @Override
    public boolean hasNext() {
        if ( next == null ) {
            advance();
        }

        return next != null;
    }


    @Override
    public ShardEntryGroup next() {
        if ( !hasNext() ) {
            throw new NoSuchElementException( "No more elements exist in iterator" );
        }


        final ShardEntryGroup toReturn = next;

        next = null;

        return toReturn;
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException( "Remove is not supported" );
    }


    /**
     * Advance to the next element
     */
    private void advance() {

        /**
         * We loop through until we've exhausted our source, or we have 2 elements, which means
         * they're > min time allocation from one another
         */
        while ( sourceIterator.hasNext() ) {

            if(next == null){
                next = new ShardEntryGroup( minDelta );
            }

            final Shard shard = sourceIterator.next();


            //we can't add this one to the entries, it doesn't fit within the delta, allocate a new one and break
            if ( next.addShard( shard ) ) {
                continue;
            }


            sourceIterator.pushback( shard );
            break;
        }


    }
}