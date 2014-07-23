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


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.usergrid.persistence.core.consistency.TimeService;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.MarkedEdge;
import org.apache.usergrid.persistence.graph.SearchByIdType;
import org.apache.usergrid.persistence.graph.exception.GraphRuntimeException;
import org.apache.usergrid.persistence.graph.impl.SimpleMarkedEdge;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.impl.NodeShardAllocationImpl;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.util.UUIDGenerator;

import com.google.common.base.Optional;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;

import static junit.framework.TestCase.assertTrue;
import static org.apache.usergrid.persistence.graph.test.util.EdgeTestUtils.createId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class NodeShardAllocationTest {


    private GraphFig graphFig;


    protected ApplicationScope scope;


    @Before
    public void setup() {
        scope = mock( ApplicationScope.class );

        Id orgId = mock( Id.class );

        when( orgId.getType() ).thenReturn( "organization" );
        when( orgId.getUuid() ).thenReturn( UUIDGenerator.newTimeUUID() );

        when( scope.getApplication() ).thenReturn( orgId );

        graphFig = mock( GraphFig.class );

        when( graphFig.getShardCacheSize() ).thenReturn( 10000l );
        when( graphFig.getShardSize() ).thenReturn( 20000l );

        final long timeout = 30000;
        when( graphFig.getShardCacheTimeout() ).thenReturn( timeout );
        when( graphFig.getShardMinDelta() ).thenReturn( timeout * 2 );
    }


//    @Test
//    public void minTime() {
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//        final NodeShardApproximation nodeShardCounterSerialization = mock( NodeShardApproximation.class );
//
//
//        final TimeService timeService = mock( TimeService.class );
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardCounterSerialization, timeService, graphFig, keyspace );
//
//
//        final long timeservicetime = System.currentTimeMillis();
//
//        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );
//
//        final long expected = timeservicetime - 2 * graphFig.getShardCacheTimeout();
//
//        final long returned = approximation.getMinTime();
//
//        assertEquals( "Correct time was returned", expected, returned );
//    }
//
//
//    @Test
//    public void noShards() {
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//
//        final NodeShardApproximation nodeShardCounterSerialization = mock( NodeShardApproximation.class );
//
//
//        final TimeService timeService = mock( TimeService.class );
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//        final MutationBatch batch = mock( MutationBatch.class );
//
//        when( keyspace.prepareMutationBatch() ).thenReturn( batch );
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardCounterSerialization, timeService, graphFig, keyspace );
//
//        final Id nodeId = createId( "test" );
//        final String type = "type";
//        final String subType = "subType";
//
//        /**
//         * Mock up returning an empty iterator, our audit shouldn't create a new shard
//         */
//        when( edgeShardSerialization
//                .getShardMetaData( same( scope ), same( nodeId ), eq( NodeType.SOURCE ), any( Optional.class ),
//                        same( type ), same( subType ) ) ).thenReturn( Collections.<Shard>emptyList().iterator() );
//
//        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.SOURCE, type, subType );
//
//        assertFalse( "No shard allocated", result );
//    }
//
//
//    @Test
//    public void existingFutureShardSameTime() {
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//        final NodeShardApproximation nodeShardCounterSerialization = mock( NodeShardApproximation.class );
//
//
//        final TimeService timeService = mock( TimeService.class );
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//
//        final MutationBatch batch = mock( MutationBatch.class );
//
//        when( keyspace.prepareMutationBatch() ).thenReturn( batch );
//
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardCounterSerialization, timeService, graphFig, keyspace );
//
//        final Id nodeId = createId( "test" );
//        final String type = "type";
//        final String subType = "subType";
//
//
//        final long timeservicetime = System.currentTimeMillis();
//
//        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );
//
//        final Shard futureShard = new Shard( 10000l, timeservicetime, compacted );
//
//        /**
//         * Mock up returning a min shard, and a future shard
//         */
//        when( edgeShardSerialization
//                .getShardMetaData( same( scope ), same( nodeId ), eq( NodeType.TARGET ), any( Optional.class ),
//                        same( type ), same( subType ) ) ).thenReturn( Arrays.asList( futureShard ).iterator() );
//
//        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.TARGET, type, subType );
//
//        assertFalse( "No shard allocated", result );
//    }
//
//
//    @Test
//    public void lowCountFutureShard() {
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//        final NodeShardApproximation nodeShardApproximation = mock( NodeShardApproximation.class );
//
//
//        final TimeService timeService = mock( TimeService.class );
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//        final MutationBatch batch = mock( MutationBatch.class );
//
//        when( keyspace.prepareMutationBatch() ).thenReturn( batch );
//
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardApproximation, timeService, graphFig, keyspace );
//
//        final Id nodeId = createId( "test" );
//        final String type = "type";
//        final String subType = "subType";
//
//
//        final long timeservicetime = System.currentTimeMillis();
//
//        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );
//
//
//        /**
//         * Mock up returning a min shard, and a future shard
//         */
//        when( edgeShardSerialization
//                .getShardMetaData( same( scope ), same( nodeId ), eq( NodeType.TARGET ), any( Optional.class ),
//                        same( type ), same( subType ) ) ).thenReturn( Arrays.asList( new Shard( 0l, 0l, compacted ) ).iterator() );
//
//
//        //return a shard size < our max by 1
//
//        final long count = graphFig.getShardSize() - 1;
//
//        when( nodeShardApproximation.getCount( scope, nodeId, NodeType.TARGET, 0l, type, subType ) )
//                .thenReturn( count );
//
//        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.TARGET, type, subType );
//
//        assertFalse( "Shard allocated", result );
//    }
//
//
//    @Test
//    public void equalCountFutureShard() {
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//        final NodeShardApproximation nodeShardApproximation = mock( NodeShardApproximation.class );
//
//
//        final TimeService timeService = mock( TimeService.class );
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//        final MutationBatch batch = mock( MutationBatch.class );
//
//        when( keyspace.prepareMutationBatch() ).thenReturn( batch );
//
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardApproximation, timeService, graphFig, keyspace );
//
//        final Id nodeId = createId( "test" );
//        final String type = "type";
//        final String subType = "subType";
//
//
//        final long timeservicetime = System.currentTimeMillis();
//
//        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );
//
//
//        /**
//         * Mock up returning a min shard
//         */
//        when( edgeShardSerialization
//                .getShardMetaData( same( scope ), same( nodeId ), eq( NodeType.SOURCE ), any( Optional.class ),
//                        same( type ), same( subType ) ) ).thenReturn( Arrays.asList( new Shard( 0l, 0l, compacted ) ).iterator() );
//
//
//        final long shardCount = graphFig.getShardSize();
//
//        //return a shard size equal to our max
//        when( nodeShardApproximation.getCount( scope, nodeId, NodeType.SOURCE, 0l, type, subType ) )
//                .thenReturn( shardCount );
//
//        ArgumentCaptor<Long> shardValue = ArgumentCaptor.forClass( Long.class );
//        ArgumentCaptor<Long> timestampValue = ArgumentCaptor.forClass( Long.class );
//
//
//        //mock up our mutation
//        when( edgeShardSerialization
//                .writeShardMeta( same( scope ), same( nodeId ), eq( NodeType.SOURCE ), shardValue.capture(),
//                        timestampValue.capture(), same( type ), same( subType ) ) )
//                .thenReturn( mock( MutationBatch.class ) );
//
//
//        final SimpleMarkedEdge returnedEdge =
//                new SimpleMarkedEdge( nodeId, type, createId( "subType" ), 10005l, false );
//        final Iterator<MarkedEdge> edgeIterator = Collections.singleton( ( MarkedEdge ) returnedEdge ).iterator();
//
//        //mock up returning the value
//        when( shardedEdgeSerialization
//                .getEdgesFromSourceByTargetType( same( edgeColumnFamilies ), same( scope ), any( SearchByIdType.class ),
//                        any( Iterator.class ) ) ).thenReturn( edgeIterator );
//
//
//        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.SOURCE, type, subType );
//
//        assertTrue( "Shard allocated", result );
//
//        //check our new allocated UUID
//
//
//        final long savedTimestamp = timestampValue.getValue();
//
//
//        assertEquals( "Expected time service time", timeservicetime, savedTimestamp );
//
//
//        //now check our max value was set
//
//        final long savedShardPivot = shardValue.getValue();
//
//        assertEquals( "Expected max value to be the same", returnedEdge.getTimestamp(), savedShardPivot );
//    }
//
//
//    @Test
//    public void futureCountShardCleanup() {
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//        final NodeShardApproximation nodeShardApproximation = mock( NodeShardApproximation.class );
//
//
//        final TimeService timeService = mock( TimeService.class );
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//        final MutationBatch batch = mock( MutationBatch.class );
//
//        when( keyspace.prepareMutationBatch() ).thenReturn( batch );
//
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardApproximation, timeService, graphFig, keyspace );
//
//        final Id nodeId = createId( "test" );
//        final String type = "type";
//        final String subType = "subType";
//
//
//        /**
//         * Use the time service to generate timestamps
//         */
//        final long timeservicetime = 10000;
//
//
//        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );
//
//        assertTrue( "Shard cache mocked", graphFig.getShardCacheTimeout() > 0 );
//
//
//        /**
//         * Simulates clock drift when 2 nodes create future shards near one another
//         */
//        final long minDelta = graphFig.getShardMinDelta();
//
//
//        final Shard minShard = new Shard( 0l, 0l, compacted );
//
//        //a shard that isn't our minimum, but exists after compaction
//        final Shard compactedShard = new Shard( 5000, 1000, compacted );
//
//        /**
//         * Simulate different node time allocation
//         */
//
//        final long minTime = 10000;
//        //our second shard is the "oldest", and hence should be returned in the iterator.  Future shard 1 and 3
//        // should be removed
//
//        //this should get dropped, It's allocated after future shard2 even though the time is less
//        final Shard futureShard1 = new Shard( 10000, minTime + minDelta, compacted );
//
//        //should get kept.
//        final Shard futureShard2 = new Shard( 10005, minTime, compacted );
//
//        //should be removed
//        final Shard futureShard3 = new Shard( 10010, minTime + minDelta / 2, compacted );
//
//        /**
//         * Mock up returning a min shard
//         */
//        when( edgeShardSerialization
//                .getShardMetaData( same( scope ), same( nodeId ), eq( NodeType.TARGET ), any( Optional.class ),
//                        same( type ), same( subType ) ) ).thenReturn(
//                Arrays.asList( futureShard3, futureShard2, futureShard1, compactedShard, minShard ).iterator() );
//
//
//        ArgumentCaptor<Long> newLongValue = ArgumentCaptor.forClass( Long.class );
//
//
//        //mock up our mutation
//        when( edgeShardSerialization
//                .removeShardMeta( same( scope ), same( nodeId ), eq( NodeType.TARGET ), newLongValue.capture(),
//                        same( type ), same( subType ) ) ).thenReturn( mock( MutationBatch.class ) );
//
//
//        final Iterator<ShardEntryGroup> result =
//                approximation.getShards( scope, nodeId, NodeType.TARGET, Optional.<Shard>absent(), type, subType );
//
//
//        assertTrue( "Shards present", result.hasNext() );
//
//
//        ShardEntryGroup shardEntryGroup = result.next();
//
//        assertEquals( "Future shard returned", futureShard1, shardEntryGroup.getCompactionTarget() );
//
//
//        //now verify all 4 are in this group.  This is because the first shard (0,0) (n-1_ may be the only shard other
//        //nodes see while we're rolling our state.  This means it should be read and merged from as well
//
//        Collection<Shard> writeShards = shardEntryGroup.getWriteShards();
//
//        assertEquals( "Shard size as expected", 4, writeShards.size() );
//
//        assertTrue( writeShards.contains( futureShard1 ) );
//        assertTrue( writeShards.contains( futureShard2 ) );
//        assertTrue( writeShards.contains( futureShard3 ) );
//        assertTrue( writeShards.contains( compactedShard ) );
//
//
//        Collection<Shard> readShards = shardEntryGroup.getReadShards( minTime + minDelta );
//
//        assertEquals( "Shard size as expected", 4, readShards.size() );
//
//        assertTrue( readShards.contains( futureShard1 ) );
//        assertTrue( readShards.contains( futureShard2 ) );
//        assertTrue( readShards.contains( futureShard3 ) );
//        assertTrue( readShards.contains( compactedShard ) );
//
//
//        assertTrue( "Shards present", result.hasNext() );
//
//        shardEntryGroup = result.next();
//
//        writeShards = shardEntryGroup.getWriteShards();
//
//
//        assertTrue( "Previous shard present", writeShards.contains( minShard ) );
//
//
//        writeShards = shardEntryGroup.getReadShards( minTime + minDelta );
//
//
//        assertTrue( "Previous shard present", writeShards.contains( minShard ) );
//
//
//        assertFalse( "No shards left", result.hasNext() );
//    }
//
//
//    @Test
//    public void noShardsReturns() {
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//        final NodeShardApproximation nodeShardApproximation = mock( NodeShardApproximation.class );
//
//
//        final TimeService timeService = mock( TimeService.class );
//
//        when( timeService.getCurrentTime() ).thenReturn( 10000l );
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//        final MutationBatch batch = mock( MutationBatch.class );
//
//        when( keyspace.prepareMutationBatch() ).thenReturn( batch );
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardApproximation, timeService, graphFig, keyspace );
//
//        final Id nodeId = createId( "test" );
//        final String type = "type";
//        final String subType = "subType";
//
//        /**
//         * Mock up returning an empty iterator, our audit shouldn't create a new shard
//         */
//        when( edgeShardSerialization
//                .getShardMetaData( same( scope ), same( nodeId ), eq( NodeType.TARGET ), any( Optional.class ),
//                        same( type ), same( subType ) ) ).thenReturn( Collections.<Shard>emptyList().iterator() );
//
//        final Iterator<ShardEntryGroup> result =
//                approximation.getShards( scope, nodeId, NodeType.TARGET, Optional.<Shard>absent(), type, subType );
//
//
//        ShardEntryGroup shardEntryGroup = result.next();
//
//        final Shard expected = new Shard( 0, 0, compacted );
//
//        assertEquals( "Future shard returned", expected, shardEntryGroup.getCompactionTarget() );
//
//
//        //now verify all 4 are in this group.  This is because the first shard (0,0) (n-1_ may be the only shard other
//        //nodes see while we're rolling our state.  This means it should be read and merged from as well
//
//        Collection<Shard> writeShards = shardEntryGroup.getWriteShards();
//
//        Collection<Shard> readShards = shardEntryGroup.getReadShards( 10000l );
//
//
//        assertTrue( "0 shard allocated", writeShards.contains( expected ) );
//
//        assertTrue( "0 shard allocated", readShards.contains( expected ) );
//
//
//        assertFalse( "No shard allocated", result.hasNext() );
//    }
//
//
//    @Test
//    public void invalidConfiguration() {
//
//        final GraphFig graphFig = mock( GraphFig.class );
//
//        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );
//
//        final EdgeColumnFamilies edgeColumnFamilies = mock( EdgeColumnFamilies.class );
//
//        final ShardedEdgeSerialization shardedEdgeSerialization = mock( ShardedEdgeSerialization.class );
//
//        final NodeShardApproximation nodeShardApproximation = mock( NodeShardApproximation.class );
//
//
//        /**
//         * Return 100000 milliseconds
//         */
//        final TimeService timeService = mock( TimeService.class );
//
//        final long time = 100000l;
//
//        when( timeService.getCurrentTime() ).thenReturn( time );
//
//
//        final long cacheTimeout = 30000l;
//
//        when( graphFig.getShardCacheTimeout() ).thenReturn( 30000l );
//
//
//        final long tooSmallDelta = ( long ) ( ( cacheTimeout * 2 ) * .99 );
//
//        when( graphFig.getShardMinDelta() ).thenReturn( tooSmallDelta );
//
//
//        final Keyspace keyspace = mock( Keyspace.class );
//
//        final MutationBatch batch = mock( MutationBatch.class );
//
//        when( keyspace.prepareMutationBatch() ).thenReturn( batch );
//
//
//        NodeShardAllocation approximation =
//                new NodeShardAllocationImpl( edgeShardSerialization, edgeColumnFamilies, shardedEdgeSerialization,
//                        nodeShardApproximation, timeService, graphFig, keyspace );
//
//
//        /**
//         * Should throw an exception
//         */
//        try {
//            approximation.getMinTime();
//            fail( "Should have thrown a GraphRuntimeException" );
//        }
//        catch ( GraphRuntimeException gre ) {
//            //swallow
//        }
//
//        //now test something that passes.
//
//        final long minDelta = cacheTimeout * 2;
//
//        when( graphFig.getShardMinDelta() ).thenReturn( minDelta );
//
//        long returned = approximation.getMinTime();
//
//        long expectedReturned = time - minDelta;
//
//        assertEquals( expectedReturned, returned );
//
//        final long delta = cacheTimeout * 4;
//
//        when( graphFig.getShardMinDelta() ).thenReturn( delta );
//
//        returned = approximation.getMinTime();
//
//        expectedReturned = time - delta;
//
//        assertEquals( expectedReturned, returned );
//    }
}
