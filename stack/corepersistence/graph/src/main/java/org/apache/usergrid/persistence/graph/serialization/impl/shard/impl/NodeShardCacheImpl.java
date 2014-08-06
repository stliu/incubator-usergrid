/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.usergrid.persistence.graph.serialization.impl.shard.impl;


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.usergrid.persistence.core.consistency.TimeService;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.exception.GraphRuntimeException;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeShardAllocation;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeShardCache;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeType;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.Shard;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardEntryGroup;
import org.apache.usergrid.persistence.graph.serialization.util.IterableUtil;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.inject.Inject;


/**
 * Simple implementation of the shard.  Uses a local Guava shard with a timeout.  If a value is not present in the
 * shard, it will need to be searched via cassandra.
 */
public class NodeShardCacheImpl implements NodeShardCache {

    private final NodeShardAllocation nodeShardAllocation;
    private final GraphFig graphFig;
    private final TimeService timeservice;

    private LoadingCache<CacheKey, CacheEntry> graphs;


    /**
     *  @param nodeShardAllocation
     * @param graphFig
     * @param timeservice
     */
    @Inject
    public NodeShardCacheImpl( final NodeShardAllocation nodeShardAllocation, final GraphFig graphFig,
                               final TimeService timeservice ) {

        Preconditions.checkNotNull( nodeShardAllocation, "nodeShardAllocation is required" );
        Preconditions.checkNotNull( graphFig, "consistencyFig is required" );
        Preconditions.checkNotNull( timeservice, "timeservice is required" );

        this.nodeShardAllocation = nodeShardAllocation;
        this.graphFig = graphFig;
        this.timeservice = timeservice;

        /**
         * Add our listener to reconstruct the shard
         */
        this.graphFig.addPropertyChangeListener( new PropertyChangeListener() {
            @Override
            public void propertyChange( final PropertyChangeEvent evt ) {
                final String propertyName = evt.getPropertyName();

                if ( propertyName.equals( GraphFig.SHARD_CACHE_SIZE ) || propertyName
                        .equals( GraphFig.SHARD_CACHE_TIMEOUT ) ) {

                    updateCache();
                }
            }
        } );

        /**
         * Initialize the shard
         */
        updateCache();
    }


    @Override
    public ShardEntryGroup getWriteShards( final ApplicationScope scope, final Id nodeId, final NodeType nodeType,
                                        final long timestamp, final String... edgeType ) {


        final CacheKey key = new CacheKey( scope, nodeId, nodeType, edgeType );
        CacheEntry entry;

        try {
            entry = this.graphs.get( key );
        }
        catch ( ExecutionException e ) {
            throw new GraphRuntimeException( "Unable to load shard key for graph", e );
        }

        final ShardEntryGroup shardId = entry.getShardId( timestamp );

        if ( shardId != null ) {
            return shardId;
        }

        //if we get here, something went wrong, our shard should always have a time UUID to return to us
        throw new GraphRuntimeException( "No time UUID shard was found and could not allocate one" );
    }


    @Override
    public Iterator<ShardEntryGroup> getReadShards( final ApplicationScope scope, final Id nodeId, final NodeType nodeType,
                                                 final long maxTimestamp, final String... edgeType ) {
        final CacheKey key = new CacheKey( scope, nodeId, nodeType, edgeType );
        CacheEntry entry;

        try {
            entry = this.graphs.get( key );
        }
        catch ( ExecutionException e ) {
            throw new GraphRuntimeException( "Unable to load shard key for graph", e );
        }

        Iterator<ShardEntryGroup> iterator = entry.getShards( maxTimestamp );

        if ( iterator == null ) {
            return Collections.<ShardEntryGroup>emptyList().iterator();
        }

        return iterator;
    }


    /**
     * This is a race condition.  We could re-init the shard while another thread is reading it.  This is fine, the read
     * doesn't have to be precise.  The algorithm accounts for stale data.
     */
    private void updateCache() {

        /**
         * TODO: Validate if we swamp this during a config change it garbage collects properly
         */

        if(this.graphs != null){
            this.graphs.invalidateAll();
        }


        this.graphs = CacheBuilder.newBuilder().maximumSize( graphFig.getShardCacheSize() )
                                  .expireAfterWrite( graphFig.getShardCacheSize(), TimeUnit.MILLISECONDS ).removalListener( new ShardRemovalListener() )
                                  .build( new ShardCacheLoader() );
    }


    /**
     * Cache key for looking up items in the shard
     */
    private static class CacheKey {
        private final ApplicationScope scope;
        private final Id id;
        private final NodeType nodeType;
        private final String[] types;


        private CacheKey( final ApplicationScope scope, final Id id, final NodeType nodeType, final String[] types ) {
            this.scope = scope;
            this.id = id;
            this.nodeType = nodeType;
            this.types = types;
        }


        @Override
        public boolean equals( final Object o ) {
            if ( this == o ) {
                return true;
            }
            if ( o == null || getClass() != o.getClass() ) {
                return false;
            }

            final CacheKey cacheKey = ( CacheKey ) o;

            if ( !id.equals( cacheKey.id ) ) {
                return false;
            }
            if ( nodeType != cacheKey.nodeType ) {
                return false;
            }
            if ( !scope.equals( cacheKey.scope ) ) {
                return false;
            }
            if ( !Arrays.equals( types, cacheKey.types ) ) {
                return false;
            }

            return true;
        }


        @Override
        public int hashCode() {
            int result = scope.hashCode();
            result = 31 * result + id.hashCode();
            result = 31 * result + nodeType.hashCode();
            result = 31 * result + Arrays.hashCode( types );
            return result;
        }
    }


    /**
     * An entry for the shard.
     */
    private static class CacheEntry {
        /**
         * Get the list of all segments
         */
        private TreeMap<Long, ShardEntryGroup> shards;


        private CacheEntry( final Iterator<ShardEntryGroup> shards ) {
            this.shards = new TreeMap<>(ShardEntriesComparator.INSTANCE);

            /**
             * TODO, we need to bound this.  While I don't evision more than a thousand groups max,
             * we don't want 1 hog all our ram
             */
            for ( ShardEntryGroup shard : IterableUtil.wrap( shards ) ) {
                this.shards.put(shard.getMinShard().getShardIndex() , shard );
            }
        }


        /**
         * Get the shard's long
         */
        public ShardEntryGroup getShardId( final Long seek ) {
            final Long entry = getShardEntriesForValue( seek );


            return shards.get( entry );
        }


        /**
         * Get all shards <= this one in decending order
         */
        public Iterator<ShardEntryGroup> getShards( final Long maxShard ) {
           final Long entry = getShardEntriesForValue( maxShard );


            return shards.tailMap( entry ).values().iterator();

        }


        /**
         * Get the shard entry that should hold this value
         * @param value
         * @return
         */
        private long getShardEntriesForValue(final Long value){
              return shards.lowerKey( value );
        }





        private static class ShardEntriesComparator implements Comparator<Long> {

            private static final ShardEntriesComparator INSTANCE = new ShardEntriesComparator();



            @Override
            public int compare( final Long o1, final Long o2 ) {
                return Long.compare( o1, o2 ) * -1;
            }
        }


    }

    private final class ShardCacheLoader extends CacheLoader<CacheKey, CacheEntry> {


        @Override
        public CacheEntry load( final CacheKey key ) throws Exception {


            //                                                                    /**
            //                                                                     * Perform an audit in case we need to allocate a new shard
            //                                                                     */
            //                                                                    nodeShardAllocation.auditMaxShard( key.scope,
            //                                          // key.id, key.types );
            //                                          //                          //TODO, we need to put some sort of upper
            //                                          // bounds on this, it could possibly get too large


            final Iterator<ShardEntryGroup> edges = nodeShardAllocation
                    .getShards( key.scope, key.id, key.nodeType, Optional.<Shard>absent(),
                            key.types );

            return new CacheEntry( edges );
        }
    }

    private final class ShardRemovalListener implements RemovalListener<CacheKey, CacheEntry>{

        @Override
        public void onRemoval( final RemovalNotification<CacheKey, CacheEntry> notification ) {



            CacheKey key = notification.getKey();
            CacheEntry entry = notification.getValue();


            Iterator<ShardEntryGroup> groups = entry.getShards( Long.MAX_VALUE );


            /**
             * Start at our max, then
             */

            //audit all our groups
            while(groups.hasNext()){
                ShardEntryGroup group = groups.next();

                /**
                 * We have a compaction that may need to be run, don't allocate anything
                 */
                if(!group.isCompactionPending()){
                    /**
                     * Check if we should allocate, we may want to
                     */
                    nodeShardAllocation.auditMaxShard(key.scope, key.id, key.nodeType, key.types  );
                    continue;
                }

                /**
                 * Do the compaction
                 */
                if(group.shouldCompact( timeservice.getCurrentTime() )){

                }

            }


        }
    }
}
