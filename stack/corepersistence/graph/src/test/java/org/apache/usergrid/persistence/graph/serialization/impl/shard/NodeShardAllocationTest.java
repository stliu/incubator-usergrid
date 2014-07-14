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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.usergrid.persistence.core.consistency.TimeService;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.GraphFig;
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
        when( graphFig.getShardCacheTimeout()).thenReturn( 30000l );
    }



    @Test
    public void minTime() {
        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );

        final NodeShardApproximation nodeShardCounterSerialization =
                mock( NodeShardApproximation.class );


        final TimeService timeService = mock( TimeService.class );

        final Keyspace keyspace = mock( Keyspace.class );


        NodeShardAllocation approximation =
                new NodeShardAllocationImpl( edgeShardSerialization, nodeShardCounterSerialization, timeService,
                        graphFig, keyspace );


        final long timeservicetime = System.currentTimeMillis();

        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );

        final long expected = timeservicetime - 2 * graphFig.getShardCacheTimeout();

        final long returned = approximation.getMinTime();

        assertEquals("Correct time was returned", expected, returned);
    }


    @Test
    public void noShards() {
        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );

        final NodeShardApproximation nodeShardCounterSerialization =
                mock( NodeShardApproximation.class );


        final TimeService timeService = mock( TimeService.class );

        final Keyspace keyspace = mock( Keyspace.class );

        final MutationBatch batch = mock( MutationBatch.class );

        when( keyspace.prepareMutationBatch() ).thenReturn( batch );

        NodeShardAllocation approximation =
                new NodeShardAllocationImpl( edgeShardSerialization, nodeShardCounterSerialization, timeService,
                        graphFig, keyspace );

        final Id nodeId = createId( "test" );
        final String type = "type";
        final String subType = "subType";

        /**
         * Mock up returning an empty iterator, our audit shouldn't create a new shard
         */
        when( edgeShardSerialization
                .getEdgeMetaData( same( scope ), same( nodeId ), eq( NodeType.SOURCE ), any( Optional.class ),  same( type ),
                        same( subType ) ) ).thenReturn( Collections.<Shard>emptyList().iterator() );

        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.SOURCE, type, subType );

        assertFalse( "No shard allocated", result );
    }





    @Test
    public void existingFutureShardSameTime() {
        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );

        final NodeShardApproximation nodeShardCounterSerialization =
                mock( NodeShardApproximation.class );


        final TimeService timeService = mock( TimeService.class );

        final Keyspace keyspace = mock( Keyspace.class );


        final MutationBatch batch = mock( MutationBatch.class );

        when( keyspace.prepareMutationBatch() ).thenReturn( batch );


        NodeShardAllocation approximation =
                new NodeShardAllocationImpl( edgeShardSerialization, nodeShardCounterSerialization, timeService,
                        graphFig, keyspace );

        final Id nodeId = createId( "test" );
        final String type = "type";
        final String subType = "subType";


        final long timeservicetime = System.currentTimeMillis();

        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );

        final Shard futureShard =  new Shard(10000l, timeservicetime) ;

        /**
         * Mock up returning a min shard, and a future shard
         */
        when( edgeShardSerialization
                .getEdgeMetaData( same( scope ), same( nodeId ), eq( NodeType.TARGET), any( Optional.class ),  same( type ),
                        same( subType ) ) ).thenReturn( Arrays.asList( futureShard ).iterator() );

        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.TARGET,  type, subType );

        assertFalse( "No shard allocated", result );
    }


    @Test
    public void lowCountFutureShard() {
        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );

        final NodeShardApproximation nodeShardApproximation =
                mock( NodeShardApproximation.class );


        final TimeService timeService = mock( TimeService.class );

        final Keyspace keyspace = mock( Keyspace.class );

        final MutationBatch batch = mock( MutationBatch.class );

        when( keyspace.prepareMutationBatch() ).thenReturn( batch );


        NodeShardAllocation approximation =
                new NodeShardAllocationImpl( edgeShardSerialization, nodeShardApproximation, timeService,
                        graphFig, keyspace );

        final Id nodeId = createId( "test" );
        final String type = "type";
        final String subType = "subType";


        final long timeservicetime = System.currentTimeMillis();

        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );


        /**
         * Mock up returning a min shard, and a future shard
         */
        when( edgeShardSerialization
                .getEdgeMetaData( same( scope ), same( nodeId ), eq(NodeType.TARGET), any( Optional.class ),  same( type ),
                        same( subType ) ) ).thenReturn( Arrays.asList( new Shard(0l, 0l) ).iterator() );


        //return a shard size < our max by 1

        final long count = graphFig.getShardSize() - 1;

        when( nodeShardApproximation.getCount(scope, nodeId, NodeType.TARGET, 0l, type, subType )).thenReturn( count );

        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.TARGET, type, subType );

        assertFalse( "Shard allocated", result );
    }


    @Test
    public void equalCountFutureShard() {
        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );

        final NodeShardApproximation nodeShardApproximation =
                mock( NodeShardApproximation.class );


        final TimeService timeService = mock( TimeService.class );

        final Keyspace keyspace = mock( Keyspace.class );

        final MutationBatch batch = mock(MutationBatch.class);

        when(keyspace.prepareMutationBatch()).thenReturn( batch );


        NodeShardAllocation approximation =
                new NodeShardAllocationImpl( edgeShardSerialization, nodeShardApproximation, timeService,
                        graphFig, keyspace );

        final Id nodeId = createId( "test" );
        final String type = "type";
        final String subType = "subType";


        final long timeservicetime = System.currentTimeMillis();

        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );


        /**
         * Mock up returning a min shard
         */
        when( edgeShardSerialization
                .getEdgeMetaData( same( scope ), same( nodeId ), eq(NodeType.SOURCE), any( Optional.class ),  same( type ),
                        same( subType ) ) ).thenReturn( Arrays.asList( new Shard(0l, 0l) ).iterator() );


        final long shardCount = graphFig.getShardSize();

        //return a shard size equal to our max
        when( nodeShardApproximation
                .getCount(   scope , nodeId, NodeType.SOURCE, 0l,type , subType  ))
                .thenReturn( shardCount );

        ArgumentCaptor<Long> shardValue = ArgumentCaptor.forClass( Long.class );
        ArgumentCaptor<Long> timestampValue = ArgumentCaptor.forClass( Long.class );


        //mock up our mutation
        when( edgeShardSerialization
                .writeEdgeMeta( same( scope ), same( nodeId ), eq(NodeType.SOURCE), shardValue.capture(), timestampValue.capture(), same( type ), same( subType ) ) )
                .thenReturn( mock( MutationBatch.class ) );


        final boolean result = approximation.auditMaxShard( scope, nodeId, NodeType.SOURCE,  type, subType );

        assertTrue( "Shard allocated", result );

        //check our new allocated UUID


        final long savedTimestamp = timestampValue.getValue();





        assertEquals( "Expected time service time", timeservicetime, savedTimestamp );

        //now check our max value was set
    }




    @Test
    public void futureCountShardCleanup() {
        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );

        final NodeShardApproximation nodeShardApproximation =
                mock( NodeShardApproximation.class );


        final TimeService timeService = mock( TimeService.class );

        final Keyspace keyspace = mock( Keyspace.class );

        final MutationBatch batch = mock(MutationBatch.class);

        when(keyspace.prepareMutationBatch()).thenReturn( batch );


        NodeShardAllocation approximation =
                new NodeShardAllocationImpl( edgeShardSerialization, nodeShardApproximation, timeService,
                        graphFig, keyspace );

        final Id nodeId = createId( "test" );
        final String type = "type";
        final String subType = "subType";


        /**
         * Use the time service to generate UUIDS
         */
        final long timeservicetime = System.currentTimeMillis();


        when( timeService.getCurrentTime() ).thenReturn( timeservicetime );

        assertTrue("Shard cache mocked", graphFig.getShardCacheTimeout() > 0);


        /**
         * Simulates clock drift when 2 nodes create future shards near one another
         */
        final long futureTime = timeService.getCurrentTime()  + 2 * graphFig.getShardCacheTimeout();


        final Shard minShard = new Shard(0l, 0l);

        /**
         * Simulate slow node
         */

        //our second shard is the "oldest", and hence should be returned in the iterator.  Future shard 1 and 3 should be removed
        final Shard futureShard1 = new Shard(futureTime - 1, timeservicetime+1000);

        final Shard futureShard2 = new Shard(futureTime + 10000, timeservicetime);

        final Shard futureShard3 = new Shard(futureShard2.getShardIndex() + 10000, timeservicetime+2000);

        /**
         * Mock up returning a min shard
         */
        when( edgeShardSerialization
                .getEdgeMetaData( same( scope ), same( nodeId ), eq(NodeType.TARGET), any( Optional.class ), same( type ),
                        same( subType ) ) ).thenReturn( Arrays.asList(futureShard3, futureShard2, futureShard1, minShard).iterator() );



        ArgumentCaptor<Long> newLongValue = ArgumentCaptor.forClass( Long.class );




        //mock up our mutation
        when( edgeShardSerialization
                .removeEdgeMeta( same( scope ), same( nodeId ), eq(NodeType.TARGET), newLongValue.capture(), same( type ), same( subType ) ) )
                .thenReturn( mock( MutationBatch.class ) );


        final Iterator<Shard>
                result = approximation.getShards( scope, nodeId, NodeType.TARGET, Optional.<Shard>absent(), type, subType );


        assertTrue( "Shards present", result.hasNext() );

        assertEquals("Only single next shard returned", futureShard2,  result.next());

        assertTrue("Shards present", result.hasNext());

        assertEquals("Previous shard present", 0l, result.next().getShardIndex());

        assertFalse("No shards left", result.hasNext());

        /**
         * Now we need to verify that both our mutations have been added
         */

        List<Long> values = newLongValue.getAllValues();

        assertEquals("2 values removed", 2,  values.size());

        assertEquals("Deleted Max Future", futureShard1.getShardIndex(), values.get( 0 ).longValue());
        assertEquals("Deleted Next Future", futureShard3.getShardIndex(), values.get( 1 ).longValue());

    }




    @Test
    public void noShardsReturns() {
        final EdgeShardSerialization edgeShardSerialization = mock( EdgeShardSerialization.class );

        final NodeShardApproximation nodeShardApproximation =
                mock( NodeShardApproximation.class );


        final TimeService timeService = mock( TimeService.class );

        final Keyspace keyspace = mock( Keyspace.class );

        final MutationBatch batch = mock( MutationBatch.class );

        when( keyspace.prepareMutationBatch() ).thenReturn( batch );

        NodeShardAllocation approximation =
                new NodeShardAllocationImpl( edgeShardSerialization, nodeShardApproximation, timeService,
                        graphFig, keyspace );

        final Id nodeId = createId( "test" );
        final String type = "type";
        final String subType = "subType";

        /**
         * Mock up returning an empty iterator, our audit shouldn't create a new shard
         */
        when( edgeShardSerialization
                .getEdgeMetaData( same( scope ), same( nodeId ), eq(NodeType.TARGET), any( Optional.class ),  same( type ),
                        same( subType ) ) ).thenReturn( Collections.<Shard>emptyList().iterator() );

        final Iterator<Shard> result = approximation.getShards( scope, nodeId, NodeType.TARGET,  Optional.<Shard>absent(), type,
                subType );

        assertEquals("0 shard allocated", 0l, result.next().getShardIndex());

        assertFalse( "No shard allocated", result.hasNext() );
    }

}
