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
package org.apache.usergrid.persistence.graph.serialization.impl.shard;


import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;


/**
 * There are cases where we need to read or write to more than 1 shard.  This object encapsulates
 * a set of shards that should be written to and read from.  All reads should combine the data sets from
 * all shards in the group, and writes should be written to each shard.  Once the shard can safely be compacted
 * a background process should be triggered to remove additional shards and make seeks faster.  This multiread/write
 * should only occur during the time period of the delta (in milliseconds), after which the next read will asynchronously compact the
 * shards into a single shard.
 */
public class ShardEntryGroup {


    private TreeMap<Long, Shard> shards;

    private Shard minShardByIndex;

    private final long delta;

    private Shard neighbor;


    /**
     * The max delta we accept in milliseconds for create time to be considered a member of this group
     * @param delta
     */
    public ShardEntryGroup( final long delta ) {
        this.delta = delta;
        this.shards = new TreeMap<>( ShardTimeComparator.INSTANCE );
    }


    /**
     * Only add a shard if the created timestamp is within the delta of one of the entries
     * @param shard
     * @return
     */
    public boolean addShard(final Shard shard){

        //compare the time and see if it falls withing any of the elements based on their timestamp
        final long shardCreateTime = shard.getCreatedTime();

        final Long lessThanKey = shards.floorKey( shardCreateTime );

        final Long greaterThanKey = shards.ceilingKey( shardCreateTime );


        final long lessThanDelta = shardCreateTime - lessThanKey;

        final long greaterThanDelta = greaterThanKey - shardCreateTime;

        if(lessThanDelta < delta || greaterThanDelta < delta ){
            this.shards.put( shardCreateTime, shard );

            if(shard.compareTo( minShardByIndex ) < 0){
                minShardByIndex = shard;
            }

            return true;
        }

        return false;
    }


    /**
     * Add the n-1 shard to the set.  This is required, because nodes that have not yet updated their
     * shard caches can be writing reading to the n-1 node only
     *
     * @param shard The shard to possibly add as a neighbor
     * @return True if this shard as added as a neighbor, false otherwise
     */
    public boolean setNeighbor( final Shard shard ){

        //not in the transition state don't set the neighbor, it will slow seeks down
        if(!isRolling()){
            return false;
        }


        neighbor = shard;
        this.shards.put( shard.getCreatedTime(), shard );
        return true;

    }


    /**
     * Get the entries that we should read from.
     *
     * @return
     */
    public Collection<Shard> getReadShards(final long currentTime) {

        /**
         * The shards are still rolling (I.E can't be compacted)
         */
        if(needsCompaction( currentTime )){
            return shards.values();
        }

        return Collections.singleton(minShardByIndex);
    }


    /**
     * Get the entries, with the max shard time being first. We write to all shards until they're migrated
     *
     * @return
     */
    public Collection<Shard> getWriteShards() {
        return shards.values();
    }


    /**
     * Get the shard all compactions should write to
     * @return
     */
    public Shard getMergeTarget(){
        return minShardByIndex;
    }


    /**
     * Returns true if the newest created shard is path the currentTime - delta
     * @param currentTime The current system time in milliseconds
     * @return True if these shards can safely be combined into a single shard, false otherwise
     */
    public boolean needsCompaction(final long currentTime){

        /**
         * We don't have enough shards to compact, ignore
         */
        if(shards.size() < 2){
            return false;
        }


        final long maxTimestamp = shards.lastKey();


        return currentTime - delta > maxTimestamp;
    }


    /**
     * Return true if the shard is rolling.  If this is the case, we want to include the n-1 entry, since everyone
     * may not yet have it until compaction is safe to perform
     * @return
     */
    private boolean isRolling(){
       return shards.size() > 1;
    }


    /**
     * Return true if this shard can be deleted AFTER all of the data in it has been moved
     * @param shard
     * @return
     */
    public boolean canBeDeleted(final Shard shard){
        //if we're a neighbor shard (n-1) or the target compaction shard, we can't be deleted
        //we purposefully use .equals here, since 2 shards might have the same index with different timestamps (unlikely but could happen)
        if(shard == neighbor ||  getMergeTarget().equals( shard )){
            return false;
        }

        return true;
    }

    /**
     * Compares 2 shards based on create time.  Does not handle nulls intentionally
     */
    private static final class ShardTimeComparator implements Comparator<Long> {

        public static final ShardTimeComparator INSTANCE = new ShardTimeComparator();


        @Override
        public int compare( final Long o1, final Long o2 ) {
            return o1.compareTo( o2 );
        }
    }


}
