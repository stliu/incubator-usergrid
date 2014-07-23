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
import java.util.TreeSet;


/**
 * There are cases where we need to read or write to more than 1 shard.  This object encapsulates a set of shards that
 * should be written to and read from.  All reads should combine the data sets from all shards in the group, and writes
 * should be written to each shard.  Once the shard can safely be compacted a background process should be triggered to
 * remove additional shards and make seeks faster.  This multiread/write should only occur during the time period of the
 * delta (in milliseconds), after which the next read will asynchronously compact the shards into a single shard.
 */
public class ShardEntryGroup {


    private TreeSet<Shard> shards;

    private final long delta;

    private Shard compactionTarget;


    private long maxCreatedTime;


    /**
     * The max delta we accept in milliseconds for create time to be considered a member of this group
     */
    public ShardEntryGroup( final long delta ) {
        this.delta = delta;
        this.shards = new TreeSet<>();
        this.maxCreatedTime = 0;
    }


    /**
     * Only add a shard if it is within the rules require to meet a group.  The rules are outlined below.
     *
     * Case 1)  First shard in the group, always added
     *
     * Case 2) Shard is unmerged, it should be included with it's peers since other nodes may not have it yet
     *
     * Case 3) The list contains only non compacted shards, and this is last and and merged.  It is considered a lower
     * bound
     */
    public boolean addShard( final Shard shard ) {

        //shards can be ar

        //compare the time and see if it falls withing any of the elements based on their timestamp
        //        final long shardCreateTime = shard.getCreatedTime();

        //        final Long lessThanKey = shards.floorKey( shardCreateTime );
        //
        //        final Long greaterThanKey = shards.ceilingKey( shardCreateTime );
        //
        //        //first into the set
        //        if ( lessThanKey == null && greaterThanKey == null ) {
        //            addShardInternal( shard );
        //            return true;
        //        }
        //
        //        if ( lessThanKey != null && shardCreateTime - lessThanKey < delta ) {
        //            addShardInternal( shard );
        //            return true;
        //        }
        //
        //
        //        if ( greaterThanKey != null && greaterThanKey - shardCreateTime < delta ) {
        //            addShardInternal( shard );
        //
        //            return true;
        //        }

        if ( shards.size() == 0 ) {
            addShardInternal( shard );
            return true;
        }


        //shard is not compacted, or it's predecessor isn't, we should include it in this group
        if ( !shard.isCompacted() || !shards.last().isCompacted() ) {
            addShardInternal( shard );
            return true;
        }


        return false;
    }


    /**
     * Add the shard and set the min created time
     */
    private void addShardInternal( final Shard shard ) {
        shards.add( shard );

        maxCreatedTime = Math.max( maxCreatedTime, shard.getCreatedTime() );

        //it's not a compacted shard, so it's a candidate to be the compaction target
        if ( !shard.isCompacted() && ( compactionTarget == null || shard.compareTo( compactionTarget ) < 0 ) ) {
            compactionTarget = shard;
        }
    }


    /**
     * Get the entries that we should read from.
     */
    public Collection<Shard> getReadShards() {
        return shards;
    }


    /**
     * Get the entries, with the max shard time being first. We write to all shards until they're migrated
     */
    public Collection<Shard> getWriteShards( long currentTime ) {

        /**
         * The shards in this set can be combined, we should only write to the compaction target to avoid
         * adding data to other shards
         */
        if ( shouldCompact( currentTime ) ) {
            return Collections.singleton( compactionTarget );
        }


        return shards;
    }


    /**
     * Get the shard all compactions should write to
     */
    public Shard getCompactionTarget() {
        return compactionTarget;
    }


    /**
     * Returns true if the newest created shard is path the currentTime - delta
     *
     * @param currentTime The current system time in milliseconds
     *
     * @return True if these shards can safely be combined into a single shard, false otherwise
     */
    public boolean shouldCompact( final long currentTime ) {

        /**
         * We don't have enough shards to compact, ignore
         */
        if ( shards.size() < 2 ) {
            return false;
        }

        return currentTime - delta > maxCreatedTime;
    }


    /**
     * Return true if this shard can be deleted AFTER all of the data in it has been moved
     */
    public boolean canBeDeleted( final Shard shard ) {
        //if we're a neighbor shard (n-1) or the target compaction shard, we can't be deleted
        //we purposefully use shard index comparison over .equals here, since 2 shards might have the same index with
        // different timestamps
        // (unlikely but could happen)
        return !shard.isCompacted() && ( compactionTarget != null && compactionTarget.getShardIndex() != shard
                .getShardIndex() );
    }
}
