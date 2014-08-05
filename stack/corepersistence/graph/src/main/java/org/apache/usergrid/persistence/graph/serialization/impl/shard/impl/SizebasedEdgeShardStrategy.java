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


import java.util.Iterator;

import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.EdgeShardStrategy;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeShardApproximation;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeShardCache;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeType;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardEntryGroup;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 * Simple sized based shard strategy. For now always returns the same shard.
 */
@Singleton
public class SizebasedEdgeShardStrategy implements EdgeShardStrategy {


    private final NodeShardCache shardCache;
    private final NodeShardApproximation shardApproximation;


    @Inject
    public SizebasedEdgeShardStrategy( final NodeShardCache shardCache,
                                       final NodeShardApproximation shardApproximation ) {
        this.shardCache = shardCache;
        this.shardApproximation = shardApproximation;
    }


    @Override
    public ShardEntryGroup getWriteShards( final ApplicationScope scope, final Id rowKeyId, final NodeType nodeType,
                                        final long timestamp, final String... types ) {
        return shardCache.getWriteShards( scope, rowKeyId, nodeType, timestamp, types );
    }


    @Override
    public Iterator<ShardEntryGroup> getReadShards( final ApplicationScope scope, final Id rowKeyId,
                                                 final NodeType nodeType, final long maxTimestamp,
                                                 final String... types ) {
        return shardCache.getReadShards( scope, rowKeyId, nodeType, maxTimestamp, types );
    }


    @Override
    public void increment( final ApplicationScope scope, final Id rowKeyId, final NodeType nodeType, final long shardId,
                           final long count, final String... types ) {
        shardApproximation.increment( scope, rowKeyId, nodeType, shardId, count, types );
    }
}