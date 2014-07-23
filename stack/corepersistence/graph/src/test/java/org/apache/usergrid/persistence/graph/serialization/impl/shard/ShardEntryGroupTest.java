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


import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


/**
 * Test for the group functionality
 */
public class ShardEntryGroupTest {

    @Test
    public void singleEntry() {

        final long delta = 10000;

        Shard rootShard = new Shard( 0, 0 );

        ShardEntryGroup shardEntryGroup = new ShardEntryGroup( delta );

        final boolean result = shardEntryGroup.addShard( rootShard );

        assertTrue( "Shard added", result );

        assertFalse( "Single shard cannot be deleted", shardEntryGroup.canBeDeleted( rootShard ) );

        assertSame( "Same shard for merge target", rootShard, shardEntryGroup.getMergeTarget() );

        assertFalse( "Merge cannot be run with a single shard", shardEntryGroup.needsCompaction( 0 ) );
    }


    @Test
    public void allocatedWithinDelta() {

        final long delta = 10000;

        Shard firstShard = new Shard( 1000, 1000 );

        Shard secondShard = new Shard( 1000, 1000 );


        ShardEntryGroup shardEntryGroup = new ShardEntryGroup( delta );

        final boolean result = shardEntryGroup.addShard( rootShard );

        assertTrue( "Shard added", result );

        assertFalse( "Single shard cannot be deleted", shardEntryGroup.canBeDeleted( rootShard ) );

        assertSame( "Same shard for merge target", rootShard, shardEntryGroup.getMergeTarget() );

        assertFalse( "Merge cannot be run with a single shard", shardEntryGroup.needsCompaction( 0 ) );
    }
}



