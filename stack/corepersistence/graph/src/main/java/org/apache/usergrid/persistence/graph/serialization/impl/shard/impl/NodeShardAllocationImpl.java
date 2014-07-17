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


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.collections4.iterators.PushbackIterator;

import org.apache.usergrid.persistence.core.consistency.TimeService;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.MarkedEdge;
import org.apache.usergrid.persistence.graph.exception.GraphRuntimeException;
import org.apache.usergrid.persistence.graph.impl.SimpleSearchByEdgeType;
import org.apache.usergrid.persistence.graph.impl.SimpleSearchByIdType;
import org.apache.usergrid.persistence.graph.serialization.EdgeSerialization;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.EdgeColumnFamilies;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.EdgeShardSerialization;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeShardAllocation;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeShardApproximation;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeType;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.Shard;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardEntries;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardedEdgeSerialization;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;


/**
 * Implementation of the node shard monitor and allocation
 */
public class NodeShardAllocationImpl implements NodeShardAllocation {


    private static final Logger LOG = LoggerFactory.getLogger(NodeShardAllocationImpl.class);

    private static final MinShardTimeComparator MIN_SHARD_TIME_COMPARATOR = new MinShardTimeComparator();

    private final EdgeShardSerialization edgeShardSerialization;
    private final EdgeColumnFamilies edgeColumnFamilies;
    private final ShardedEdgeSerialization shardedEdgeSerialization;
    private final NodeShardApproximation nodeShardApproximation;
    private final TimeService timeService;
    private final GraphFig graphFig;
    private final Keyspace keyspace;


    @Inject
    public NodeShardAllocationImpl( final EdgeShardSerialization edgeShardSerialization,
                                    final EdgeColumnFamilies edgeColumnFamilies,
                                    final ShardedEdgeSerialization shardedEdgeSerialization,
                                    final NodeShardApproximation nodeShardApproximation, final TimeService timeService,
                                    final GraphFig graphFig, final Keyspace keyspace ) {
        this.edgeShardSerialization = edgeShardSerialization;
        this.edgeColumnFamilies = edgeColumnFamilies;
        this.shardedEdgeSerialization = shardedEdgeSerialization;
        this.nodeShardApproximation = nodeShardApproximation;
        this.timeService = timeService;
        this.graphFig = graphFig;
        this.keyspace = keyspace;
    }


    @Override
    public Iterator<Shard> getShards( final ApplicationScope scope, final Id nodeId, final NodeType nodeType,
                                      final Optional<Shard> maxShardId, final String... edgeTypes ) {

        final Iterator<Shard> existingShards =
                edgeShardSerialization.getEdgeMetaData( scope, nodeId, nodeType, maxShardId, edgeTypes );

        final PushbackIterator<Shard> pushbackIterator = new PushbackIterator( existingShards );


        final long minConflictTime = getMinTime();


        final List<Shard> futures = new ArrayList<>();


        //loop through all shards, any shard > now+1 should be deleted
        while ( pushbackIterator.hasNext() ) {

            final Shard shard = pushbackIterator.next();

            //we're done, our current time uuid is greater than the value stored
            if ( shard.getCreatedTime() < minConflictTime ) {
                //push it back into the iterator
                pushbackIterator.pushback( shard );
                break;
            }

            futures.add( shard );
        }


        //clean up our future
        Collections.sort( futures, MIN_SHARD_TIME_COMPARATOR );


        //we have more than 1 future value, we need to remove it

        MutationBatch cleanup = keyspace.prepareMutationBatch();

        //remove all futures except the last one, it is the only value we shouldn't lazy remove
        for ( int i = 1; i < futures.size(); i++ ) {
            final Shard toRemove = futures.get( i );

            final MutationBatch batch = edgeShardSerialization
                    .removeEdgeMeta( scope, nodeId, nodeType, toRemove.getShardIndex(), edgeTypes );

            cleanup.mergeShallow( batch );
        }


        try {
            cleanup.execute();
        }
        catch ( ConnectionException e ) {
            throw new GraphRuntimeException( "Unable to remove future shards, mutation error", e );
        }


        final int futuresSize = futures.size();

        if ( futuresSize > 0 ) {
            pushbackIterator.pushback( futures.get( 0 ) );
        }


        /**
         * Nothing to iterate, return an iterator with 0.
         */
        if ( !pushbackIterator.hasNext() ) {
            pushbackIterator.pushback( new Shard( 0l, 0l ) );
        }

        return pushbackIterator;
    }


    @Override
    public boolean auditMaxShard( final ApplicationScope scope, final Id nodeId, final NodeType nodeType,
                                  final String... edgeType ) {

        final Iterator<Shard> maxShards =  edgeShardSerialization.getEdgeMetaData( scope, nodeId, nodeType, Optional.<Shard>absent(), edgeType );



        //if the first shard has already been allocated, do nothing.

        //now is already > than the max, don't do anything
        if ( !maxShards.hasNext() ) {
            return false;
        }

        final Shard maxShard = maxShards.next();



        /**
         * Nothing to do, it's been created very recently, we don't create a new one
         */
        if(maxShard.getCreatedTime() >= getMinTime()){
            return false;
        }


        /**
        * Check out if we have a count for our shard allocation
        */

        final long count =
                nodeShardApproximation.getCount( scope, nodeId, nodeType, maxShard.getShardIndex(), edgeType );

        if ( count < graphFig.getShardSize() ) {
            return false;
        }


        /**
         * Allocate the shard
         */

        Iterator<MarkedEdge> edges;

        /**
         * This is fugly, I think our allocation interface needs to get more declarative
         */
        if(nodeType == NodeType.SOURCE){

            if(edgeType.length == 1){
                edges = shardedEdgeSerialization.getEdgesFromSource(edgeColumnFamilies, scope, new SimpleSearchByEdgeType( nodeId,  edgeType[0], Long.MAX_VALUE, null), Collections.singleton( new ShardEntries(Collections.singleton( maxShard ))).iterator()) ;
            }

            else if(edgeType.length == 2){
                edges = shardedEdgeSerialization.getEdgesFromSourceByTargetType(edgeColumnFamilies, scope, new SimpleSearchByIdType( nodeId,  edgeType[0], Long.MAX_VALUE,  edgeType[1], null), Collections.singleton( new ShardEntries(Collections.singleton( maxShard ))).iterator());
            }

           else{
                throw new UnsupportedOperationException( "More than 2 edge types aren't supported" );
            }

        }    else{

            if(edgeType.length == 1){
                edges = shardedEdgeSerialization.getEdgesToTarget(edgeColumnFamilies, scope, new SimpleSearchByEdgeType( nodeId,  edgeType[0], Long.MAX_VALUE, null), Collections.singleton( new ShardEntries(Collections.singleton( maxShard ))).iterator()) ;
            }

            else if(edgeType.length == 2){
                edges = shardedEdgeSerialization.getEdgesToTargetBySourceType(edgeColumnFamilies, scope, new SimpleSearchByIdType( nodeId,  edgeType[0], Long.MAX_VALUE,  edgeType[1], null), Collections.singleton( new ShardEntries(Collections.singleton( maxShard ))).iterator());
            }

           else{
                throw new UnsupportedOperationException( "More than 2 edge types aren't supported" );
            }
        }


        if(!edges.hasNext()){
            LOG.warn( "Tried to allocate a new shard for node id {} with edge types {}, but no max value could be found in that row", nodeId, edgeType );
            return false;
        }

        //we have a next, allocate it based on the max

        MarkedEdge marked = edges.next();

        final long createTimestamp = timeService.getCurrentTime();


        try {
            this.edgeShardSerialization.writeEdgeMeta( scope, nodeId, nodeType,  marked.getTimestamp(), createTimestamp, edgeType ).execute();
        }
        catch ( ConnectionException e ) {
            throw new GraphRuntimeException( "Unable to write the new edge metadata" );
        }



        return true;
    }


    @Override
    public long getMinTime() {
        return timeService.getCurrentTime() - ( 2 * graphFig.getShardCacheTimeout() );
    }


    /**
     * Sorts by minimum time first.  If 2 times are equal, the min shard value is taken
     */
    private static final class MinShardTimeComparator implements Comparator<Shard> {

        @Override
        public int compare( final Shard s1, final Shard s2 ) {
            int result = Long.compare( s1.getCreatedTime(), s2.getCreatedTime() );

            if ( result == 0 ) {
                result = Long.compare( s1.getShardIndex(), s2.getShardIndex() );
            }

            return result;
        }
    }


}
