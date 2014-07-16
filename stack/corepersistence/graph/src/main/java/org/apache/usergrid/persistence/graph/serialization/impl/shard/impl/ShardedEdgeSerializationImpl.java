/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.usergrid.persistence.graph.serialization.impl.shard.impl;


import java.util.Iterator;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.usergrid.persistence.core.astyanax.CassandraConfig;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamily;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.core.util.ValidationUtils;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.MarkedEdge;
import org.apache.usergrid.persistence.graph.SearchByEdge;
import org.apache.usergrid.persistence.graph.SearchByEdgeType;
import org.apache.usergrid.persistence.graph.SearchByIdType;
import org.apache.usergrid.persistence.graph.impl.SimpleMarkedEdge;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.DirectedEdge;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.EdgeColumnFamilies;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.EdgeRowKey;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.EdgeShardStrategy;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeType;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.RowKey;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.RowKeyType;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.Shard;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardEntries;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.ShardedEdgeSerialization;
import org.apache.usergrid.persistence.graph.serialization.util.EdgeUtils;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.util.RangeBuilder;

import static com.google.common.base.Preconditions.checkNotNull;


@Singleton
public class ShardedEdgeSerializationImpl implements ShardedEdgeSerialization {

    protected final Keyspace keyspace;
    protected final CassandraConfig cassandraConfig;
    protected final GraphFig graphFig;
    protected final EdgeShardStrategy writeEdgeShardStrategy;


    @Inject
    public ShardedEdgeSerializationImpl( final Keyspace keyspace, final CassandraConfig cassandraConfig,
                                         final GraphFig graphFig, final EdgeShardStrategy writeEdgeShardStrategy ) {

        checkNotNull( "keyspace required", keyspace );
        checkNotNull( "cassandraConfig required", cassandraConfig );
        checkNotNull( "consistencyFig required", graphFig );
        checkNotNull( "writeEdgeShardStrategy required", writeEdgeShardStrategy );


        this.keyspace = keyspace;
        this.cassandraConfig = cassandraConfig;
        this.graphFig = graphFig;
        this.writeEdgeShardStrategy = writeEdgeShardStrategy;
    }


    @Override
    public MutationBatch writeEdge( final EdgeColumnFamilies columnFamilies, final ApplicationScope scope,
                                    final MarkedEdge markedEdge, final UUID timestamp ) {
        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateEdge( markedEdge );
        ValidationUtils.verifyTimeUuid( timestamp, "timestamp" );


        final MutationBatch batch = keyspace.prepareMutationBatch().withConsistencyLevel( cassandraConfig.getWriteCL() )
                                            .withTimestamp( timestamp.timestamp() );

        final boolean isDeleted = markedEdge.isDeleted();


        doWrite( columnFamilies, scope, markedEdge, new RowOp<RowKey>() {
            @Override
            public void writeEdge( final MultiTennantColumnFamily<ApplicationScope, RowKey, DirectedEdge> columnFamily,
                                   final RowKey rowKey, final DirectedEdge edge ) {
                batch.withRow( columnFamily, ScopedRowKey.fromKey( scope, rowKey ) ).putColumn( edge, isDeleted );
            }


            @Override
            public void countEdge( final Id rowId, final NodeType nodeType, final long shardId,
                                   final String... types ) {
                if ( !isDeleted ) {
                    writeEdgeShardStrategy.increment( scope, rowId, nodeType, shardId, 1l, types );
                }
            }


            @Override
            public void writeVersion( final MultiTennantColumnFamily<ApplicationScope, EdgeRowKey, Long> columnFamily,
                                      final EdgeRowKey rowKey, final long timestamp ) {
                batch.withRow( columnFamily, ScopedRowKey.fromKey( scope, rowKey ) ).putColumn( timestamp, isDeleted );
            }
        } );


        return batch;
    }


    @Override
    public MutationBatch deleteEdge( final EdgeColumnFamilies columnFamilies, final ApplicationScope scope,
                                     final MarkedEdge markedEdge, final UUID timestamp ) {
        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateEdge( markedEdge );
        ValidationUtils.verifyTimeUuid( timestamp, "timestamp" );


        final MutationBatch batch = keyspace.prepareMutationBatch().withConsistencyLevel( cassandraConfig.getWriteCL() )
                                            .withTimestamp( timestamp.timestamp() );


        doWrite( columnFamilies, scope, markedEdge, new RowOp<RowKey>() {
            @Override
            public void writeEdge( final MultiTennantColumnFamily<ApplicationScope, RowKey, DirectedEdge> columnFamily,
                                   final RowKey rowKey, final DirectedEdge edge ) {
                batch.withRow( columnFamily, ScopedRowKey.fromKey( scope, rowKey ) ).deleteColumn( edge );
            }


            @Override
            public void countEdge( final Id rowId, final NodeType nodeType, final long shardId,
                                   final String... types ) {
                writeEdgeShardStrategy.increment( scope, rowId, nodeType, shardId, -1, types );
            }


            @Override
            public void writeVersion( final MultiTennantColumnFamily<ApplicationScope, EdgeRowKey, Long> columnFamily,
                                      final EdgeRowKey rowKey, final long timestamp ) {
                batch.withRow( columnFamily, ScopedRowKey.fromKey( scope, rowKey ) ).deleteColumn( timestamp );
            }
        } );


        return batch;
    }


    /**
     * EdgeWrite the edges internally
     *
     * @param scope The scope to encapsulate
     * @param edge The edge to write
     * @param op The row operation to invoke
     */
    private void doWrite( final EdgeColumnFamilies columnFamilies, final ApplicationScope scope, final MarkedEdge edge,
                          final RowOp op ) {
        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateEdge( edge );

        final Id sourceNodeId = edge.getSourceNode();
        final String sourceNodeType = sourceNodeId.getType();
        final Id targetNodeId = edge.getTargetNode();
        final String targetNodeType = targetNodeId.getType();
        final long timestamp = edge.getTimestamp();
        final String type = edge.getType();


        /**
         * Key in the serializers based on the edge
         */


        /**
         * write edges from source->target
         */


        final DirectedEdge sourceEdge = new DirectedEdge( targetNodeId, timestamp );

        final ShardEntries sourceRowKeyShard =
                writeEdgeShardStrategy.getWriteShards( scope, sourceNodeId, NodeType.SOURCE, timestamp, type );

        final MultiTennantColumnFamily<ApplicationScope, RowKey, DirectedEdge> sourceCf =
                columnFamilies.getSourceNodeCfName();


        for ( Shard shard : sourceRowKeyShard.getEntries() ) {

            final long shardId = shard.getShardIndex();
            final RowKey sourceRowKey = new RowKey( sourceNodeId, type, shardId );
            op.writeEdge( sourceCf, sourceRowKey, sourceEdge );
            op.countEdge( sourceNodeId, NodeType.SOURCE, shardId, type );
        }


        final ShardEntries sourceWithTypeRowKeyShard = writeEdgeShardStrategy
                .getWriteShards( scope, sourceNodeId, NodeType.SOURCE, timestamp, type, targetNodeType );

        final MultiTennantColumnFamily<ApplicationScope, RowKeyType, DirectedEdge> targetCf =
                columnFamilies.getSourceNodeTargetTypeCfName();

        for ( Shard shard : sourceWithTypeRowKeyShard.getEntries() ) {

            final long shardId = shard.getShardIndex();
            final RowKeyType sourceRowKeyType = new RowKeyType( sourceNodeId, type, targetNodeId, shardId );

            op.writeEdge( targetCf, sourceRowKeyType, sourceEdge );
            op.countEdge( sourceNodeId, NodeType.SOURCE, shardId, type, targetNodeType );
        }


        /**
         * write edges from target<-source
         */

        final DirectedEdge targetEdge = new DirectedEdge( sourceNodeId, timestamp );


        final ShardEntries targetRowKeyShard =
                writeEdgeShardStrategy.getWriteShards( scope, targetNodeId, NodeType.TARGET, timestamp, type );

        final MultiTennantColumnFamily<ApplicationScope, RowKey, DirectedEdge> sourceByTargetCf =
                columnFamilies.getTargetNodeCfName();

        for ( Shard shard : targetRowKeyShard.getEntries() ) {
            final long shardId = shard.getShardIndex();
            final RowKey targetRowKey = new RowKey( targetNodeId, type, shardId );

            op.writeEdge( sourceByTargetCf, targetRowKey, targetEdge );
            op.countEdge( targetNodeId, NodeType.TARGET, shardId, type );
        }


        final ShardEntries targetWithTypeRowKeyShard = writeEdgeShardStrategy
                .getWriteShards( scope, targetNodeId, NodeType.TARGET, timestamp, type, sourceNodeType );

        final MultiTennantColumnFamily<ApplicationScope, RowKeyType, DirectedEdge> targetBySourceCf =
                columnFamilies.getTargetNodeSourceTypeCfName();


        for ( Shard shard : targetWithTypeRowKeyShard.getEntries() ) {

            final long shardId = shard.getShardIndex();

            final RowKeyType targetRowKeyType = new RowKeyType( targetNodeId, type, sourceNodeId, shardId );


            op.writeEdge( targetBySourceCf, targetRowKeyType, targetEdge );
            op.countEdge( targetNodeId, NodeType.TARGET, shardId, type, sourceNodeType );
        }

        /**
         * Always a 0l shard, we're hard limiting 2b timestamps for the same edge
         */
        final EdgeRowKey edgeRowKey = new EdgeRowKey( sourceNodeId, type, targetNodeId, 0l );


        /**
         * Write this in the timestamp log for this edge of source->target
         */
        op.writeVersion( columnFamilies.getGraphEdgeVersions(), edgeRowKey, timestamp );
    }


    @Override
    public Iterator<MarkedEdge> getEdgeVersions( final EdgeColumnFamilies columnFamilies, final ApplicationScope scope,
                                                 final SearchByEdge search, final Iterator<ShardEntries> shards ) {
        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateSearchByEdge( search );

        final Id targetId = search.targetNode();
        final Id sourceId = search.sourceNode();
        final String type = search.getType();
        final long maxTimestamp = search.getMaxTimestamp();
        final MultiTennantColumnFamily<ApplicationScope, EdgeRowKey, Long> columnFamily =
                columnFamilies.getGraphEdgeVersions();
        final Serializer<Long> serializer = columnFamily.getColumnSerializer();

        final EdgeSearcher<EdgeRowKey, Long, MarkedEdge> searcher =
                new EdgeSearcher<EdgeRowKey, Long, MarkedEdge>( scope, maxTimestamp, search.last(), shards ) {

                    @Override
                    protected Serializer<Long> getSerializer() {
                        return serializer;
                    }


                    @Override
                    public void setRange( final RangeBuilder builder ) {


                        if ( last.isPresent() ) {
                            super.setRange( builder );
                            return;
                        }

                        //start seeking at a value < our max version
                        builder.setStart( maxTimestamp );
                    }


                    @Override
                    protected EdgeRowKey generateRowKey( long shard ) {
                        return new EdgeRowKey( sourceId, type, targetId, shard );
                    }


                    @Override
                    protected Long getStartColumn( final Edge last ) {
                        return last.getTimestamp();
                    }


                    @Override
                    protected MarkedEdge createEdge( final Long column, final boolean marked ) {
                        return new SimpleMarkedEdge( sourceId, type, targetId, column.longValue(), marked );
                    }
                };

        return new ShardRowIterator<>( searcher, columnFamily, keyspace, cassandraConfig.getReadCL(),
                graphFig.getScanPageSize() );
    }


    @Override
    public Iterator<MarkedEdge> getEdgesFromSource( final EdgeColumnFamilies columnFamilies,
                                                    final ApplicationScope scope, final SearchByEdgeType edgeType,
                                                    final Iterator<ShardEntries> shards ) {

        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateSearchByEdgeType( edgeType );

        final Id sourceId = edgeType.getNode();
        final String type = edgeType.getType();
        final long maxTimestamp = edgeType.getMaxTimestamp();
        final MultiTennantColumnFamily<ApplicationScope, RowKey, DirectedEdge> columnFamily =
                columnFamilies.getSourceNodeCfName();
        final Serializer<DirectedEdge> serializer = columnFamily.getColumnSerializer();


        final EdgeSearcher<RowKey, DirectedEdge, MarkedEdge> searcher =
                new EdgeSearcher<RowKey, DirectedEdge, MarkedEdge>( scope, maxTimestamp, edgeType.last(), shards ) {


                    @Override
                    protected Serializer<DirectedEdge> getSerializer() {
                        return serializer;
                    }


                    @Override
                    protected RowKey generateRowKey( long shard ) {
                        return new RowKey( sourceId, type, shard );
                    }


                    @Override
                    protected DirectedEdge getStartColumn( final Edge last ) {
                        return new DirectedEdge( last.getTargetNode(), last.getTimestamp() );
                    }


                    @Override
                    protected MarkedEdge createEdge( final DirectedEdge edge, final boolean marked ) {
                        return new SimpleMarkedEdge( sourceId, type, edge.id, edge.timestamp, marked );
                    }
                };


        return new ShardRowIterator<>( searcher, columnFamily, keyspace, cassandraConfig.getReadCL(),
                graphFig.getScanPageSize() );
    }


    @Override
    public Iterator<MarkedEdge> getEdgesFromSourceByTargetType( final EdgeColumnFamilies columnFamilies,
                                                                final ApplicationScope scope,
                                                                final SearchByIdType edgeType,
                                                                final Iterator<ShardEntries> shards ) {

        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateSearchByEdgeType( edgeType );

        final Id targetId = edgeType.getNode();
        final String type = edgeType.getType();
        final String targetType = edgeType.getIdType();
        final long maxTimestamp = edgeType.getMaxTimestamp();
        final MultiTennantColumnFamily<ApplicationScope, RowKeyType, DirectedEdge> columnFamily =
                columnFamilies.getSourceNodeTargetTypeCfName();
        final Serializer<DirectedEdge> serializer = columnFamily.getColumnSerializer();


        final EdgeSearcher<RowKeyType, DirectedEdge, MarkedEdge> searcher =
                new EdgeSearcher<RowKeyType, DirectedEdge, MarkedEdge>( scope, maxTimestamp, edgeType.last(), shards ) {

                    @Override
                    protected Serializer<DirectedEdge> getSerializer() {
                        return serializer;
                    }


                    @Override
                    protected RowKeyType generateRowKey( long shard ) {
                        return new RowKeyType( targetId, type, targetType, shard );
                    }


                    @Override
                    protected DirectedEdge getStartColumn( final Edge last ) {
                        return new DirectedEdge( last.getTargetNode(), last.getTimestamp() );
                    }


                    @Override
                    protected MarkedEdge createEdge( final DirectedEdge edge, final boolean marked ) {
                        return new SimpleMarkedEdge( targetId, type, edge.id, edge.timestamp, marked );
                    }
                };

        return new ShardRowIterator( searcher, columnFamily, keyspace, cassandraConfig.getReadCL(),
                graphFig.getScanPageSize() );
    }


    @Override
    public Iterator<MarkedEdge> getEdgesToTarget( final EdgeColumnFamilies columnFamilies, final ApplicationScope scope,
                                                  final SearchByEdgeType edgeType,
                                                  final Iterator<ShardEntries> shards ) {
        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateSearchByEdgeType( edgeType );

        final Id targetId = edgeType.getNode();
        final String type = edgeType.getType();
        final long maxTimestamp = edgeType.getMaxTimestamp();
        final MultiTennantColumnFamily<ApplicationScope, RowKey, DirectedEdge> columnFamily =
                columnFamilies.getTargetNodeCfName();
        final Serializer<DirectedEdge> serializer = columnFamily.getColumnSerializer();

        final EdgeSearcher<RowKey, DirectedEdge, MarkedEdge> searcher =
                new EdgeSearcher<RowKey, DirectedEdge, MarkedEdge>( scope, maxTimestamp, edgeType.last(), shards ) {

                    @Override
                    protected Serializer<DirectedEdge> getSerializer() {
                        return serializer;
                    }


                    @Override
                    protected RowKey generateRowKey( long shard ) {
                        return new RowKey( targetId, type, shard );
                    }


                    @Override
                    protected DirectedEdge getStartColumn( final Edge last ) {
                        return new DirectedEdge( last.getSourceNode(), last.getTimestamp() );
                    }


                    @Override
                    protected MarkedEdge createEdge( final DirectedEdge edge, final boolean marked ) {
                        return new SimpleMarkedEdge( edge.id, type, targetId, edge.timestamp, marked );
                    }
                };


        return new ShardRowIterator<>( searcher, columnFamily, keyspace, cassandraConfig.getReadCL(),
                graphFig.getScanPageSize() );
    }


    @Override
    public Iterator<MarkedEdge> getEdgesToTargetBySourceType( final EdgeColumnFamilies columnFamilies,
                                                              final ApplicationScope scope,
                                                              final SearchByIdType edgeType,
                                                              final Iterator<ShardEntries> shards ) {

        ValidationUtils.validateApplicationScope( scope );
        EdgeUtils.validateSearchByEdgeType( edgeType );

        final Id targetId = edgeType.getNode();
        final String sourceType = edgeType.getIdType();
        final String type = edgeType.getType();
        final long maxTimestamp = edgeType.getMaxTimestamp();
        final MultiTennantColumnFamily<ApplicationScope, RowKeyType, DirectedEdge> columnFamily =
                columnFamilies.getTargetNodeSourceTypeCfName();
        final Serializer<DirectedEdge> serializer = columnFamily.getColumnSerializer();


        final EdgeSearcher<RowKeyType, DirectedEdge, MarkedEdge> searcher =
                new EdgeSearcher<RowKeyType, DirectedEdge, MarkedEdge>( scope, maxTimestamp, edgeType.last(), shards ) {
                    @Override
                    protected Serializer<DirectedEdge> getSerializer() {
                        return serializer;
                    }


                    @Override
                    protected RowKeyType generateRowKey( final long shard ) {
                        return new RowKeyType( targetId, type, sourceType, shard );
                    }


                    @Override
                    protected DirectedEdge getStartColumn( final Edge last ) {
                        return new DirectedEdge( last.getTargetNode(), last.getTimestamp() );
                    }


                    @Override
                    protected MarkedEdge createEdge( final DirectedEdge edge, final boolean marked ) {
                        return new SimpleMarkedEdge( edge.id, type, targetId, edge.timestamp, marked );
                    }
                };

        return new ShardRowIterator<>( searcher, columnFamily, keyspace, cassandraConfig.getReadCL(),
                graphFig.getScanPageSize() );
    }


    /**
     * Simple callback to perform puts and deletes with a common row setup code
     *
     * @param <R> The row key type
     */
    private static interface RowOp<R> {

        /**
         * Write the edge with the given data
         */
        void writeEdge( final MultiTennantColumnFamily<ApplicationScope, R, DirectedEdge> columnFamily, R rowKey,
                        DirectedEdge edge );

        /**
         * Perform the count on the edge
         */
        void countEdge( final Id rowId, NodeType type, long shardId, String... types );

        /**
         * Write the edge into the version cf
         */
        void writeVersion( final MultiTennantColumnFamily<ApplicationScope, EdgeRowKey, Long> columnFamily,
                           EdgeRowKey rowKey, long timestamp );
    }
}
