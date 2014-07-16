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


import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.marshal.BytesType;

import org.apache.usergrid.persistence.core.astyanax.CassandraConfig;
import org.apache.usergrid.persistence.core.astyanax.ColumnNameIterator;
import org.apache.usergrid.persistence.core.astyanax.ColumnParser;
import org.apache.usergrid.persistence.core.astyanax.ColumnTypes;
import org.apache.usergrid.persistence.core.astyanax.CompositeFieldSerializer;
import org.apache.usergrid.persistence.core.astyanax.IdRowCompositeSerializer;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamily;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.astyanax.OrganizationScopedRowKeySerializer;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.core.util.ValidationUtils;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.EdgeShardSerialization;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeType;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.Shard;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.CompositeBuilder;
import com.netflix.astyanax.model.CompositeParser;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.util.RangeBuilder;


@Singleton
public class EdgeShardSerializationImpl implements EdgeShardSerialization {

    /**
     * Edge shards
     */
    private static final MultiTennantColumnFamily<ApplicationScope, DirectedRowKey, Long> EDGE_SHARDS =
            new MultiTennantColumnFamily<>( "Edge_Shards",
                    new OrganizationScopedRowKeySerializer<>( new DirectedEdgeRowKeySerializer() ),
                    LongSerializer.get() );


    private static final byte HOLDER = 0x00;

    private static final ShardColumnParser COLUMN_PARSER = new ShardColumnParser();


    protected final Keyspace keyspace;
    protected final CassandraConfig cassandraConfig;
    protected final GraphFig graphFig;


    @Inject
    public EdgeShardSerializationImpl( final Keyspace keyspace, final CassandraConfig cassandraConfig,
                                       final GraphFig graphFig ) {
        this.keyspace = keyspace;
        this.cassandraConfig = cassandraConfig;
        this.graphFig = graphFig;
    }


    @Override
    public MutationBatch writeEdgeMeta( final ApplicationScope scope, final Id nodeId, final NodeType nodeType,
                                        final long shard, final long timestamp, final String... types ) {


        ValidationUtils.validateApplicationScope( scope );
        ValidationUtils.verifyIdentity( nodeId );
        Preconditions.checkArgument( shard > -1, "shardId must be greater than -1" );
        Preconditions.checkNotNull( types );

        final DirectedRowKey key = new DirectedRowKey( nodeId, nodeType, types );

        final ScopedRowKey rowKey = ScopedRowKey.fromKey( scope, key );

        final MutationBatch batch = keyspace.prepareMutationBatch();

        batch.withTimestamp( timestamp ).withRow( EDGE_SHARDS, rowKey ).putColumn( shard, HOLDER );

        return batch;
    }


    @Override
    public Iterator<Shard> getEdgeMetaData( final ApplicationScope scope, final Id nodeId, final NodeType nodeType,
                                            final Optional<Shard> start, final String... types ) {
        /**
         * If the edge is present, we need to being seeking from this
         */

        final RangeBuilder rangeBuilder = new RangeBuilder().setLimit( graphFig.getScanPageSize() );

        if ( start.isPresent() ) {
            rangeBuilder.setStart( start.get().getShardIndex() );
        }

        final DirectedRowKey key = new DirectedRowKey( nodeId, nodeType, types );

        final ScopedRowKey rowKey = ScopedRowKey.fromKey( scope, key );


        final RowQuery<ScopedRowKey<ApplicationScope, DirectedRowKey>, Long> query =
                keyspace.prepareQuery( EDGE_SHARDS ).setConsistencyLevel( cassandraConfig.getReadCL() ).getKey( rowKey )
                        .autoPaginate( true ).withColumnRange( rangeBuilder.build() );


        return new ColumnNameIterator<>( query, COLUMN_PARSER, false );
    }


    @Override
    public MutationBatch removeEdgeMeta( final ApplicationScope scope, final Id nodeId, final NodeType nodeType,
                                         final long shard, final String... types ) {

        ValidationUtils.validateApplicationScope( scope );
        ValidationUtils.verifyIdentity( nodeId );
        Preconditions.checkArgument( shard > -1, "shard must be greater than -1" );
        Preconditions.checkNotNull( types );

        final DirectedRowKey key = new DirectedRowKey( nodeId, nodeType, types );

        final ScopedRowKey rowKey = ScopedRowKey.fromKey( scope, key );

        final MutationBatch batch = keyspace.prepareMutationBatch();

        batch.withRow( EDGE_SHARDS, rowKey ).deleteColumn( shard );

        return batch;
    }


    @Override
    public Collection<MultiTennantColumnFamilyDefinition> getColumnFamilies() {


        return Collections.singleton(
                new MultiTennantColumnFamilyDefinition( EDGE_SHARDS, BytesType.class.getSimpleName(),
                        ColumnTypes.LONG_TYPE_REVERSED, BytesType.class.getSimpleName(),
                        MultiTennantColumnFamilyDefinition.CacheOption.KEYS ) );
    }


    private static class DirectedRowKey {


        private final Id nodeId;
        private final NodeType nodeType;
        private final String[] edgeTypes;


        public DirectedRowKey( final Id nodeId, final NodeType nodeType, final String[] edgeTypes ) {
            this.nodeId = nodeId;
            this.nodeType = nodeType;
            this.edgeTypes = edgeTypes;
        }
    }


    private static class DirectedEdgeRowKeySerializer implements CompositeFieldSerializer<DirectedRowKey> {

        private static final IdRowCompositeSerializer ID_SER = IdRowCompositeSerializer.get();


        @Override
        public void toComposite( final CompositeBuilder builder, final DirectedRowKey key ) {
            ID_SER.toComposite( builder, key.nodeId );

            builder.addInteger( getValue( key.nodeType ) );

            builder.addInteger( key.edgeTypes.length );

            for ( String type : key.edgeTypes ) {
                builder.addString( type );
            }
        }


        @Override
        public DirectedRowKey fromComposite( final CompositeParser composite ) {
            final Id sourceId = ID_SER.fromComposite( composite );

            final NodeType type = getType( composite.readInteger() );


            final int length = composite.readInteger();

            String[] types = new String[length];

            for ( int i = 0; i < length; i++ ) {
                types[i] = composite.readString();
            }

            return new DirectedRowKey( sourceId, type, types );
        }


        private int getValue( NodeType type ) {
            if ( type == NodeType.SOURCE ) {
                return 0;
            }

            return 1;
        }


        public NodeType getType( int value ) {
            if ( value == 0 ) {
                return NodeType.SOURCE;
            }

            return NodeType.TARGET;
        }
    }


    private static class ShardColumnParser implements ColumnParser<Long, Shard> {

        @Override
        public Shard parseColumn( final Column<Long> column ) {
            return new Shard( column.getName(), column.getTimestamp() );
        }
    }
}
