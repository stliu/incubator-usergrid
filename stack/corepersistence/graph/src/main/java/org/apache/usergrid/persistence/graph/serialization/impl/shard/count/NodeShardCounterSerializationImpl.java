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
package org.apache.usergrid.persistence.graph.serialization.impl.shard.count;


import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;

import org.apache.usergrid.persistence.core.astyanax.CassandraConfig;
import org.apache.usergrid.persistence.core.astyanax.ColumnTypes;
import org.apache.usergrid.persistence.core.astyanax.CompositeFieldSerializer;
import org.apache.usergrid.persistence.core.astyanax.IdRowCompositeSerializer;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamily;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.astyanax.OrganizationScopedRowKeySerializer;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.exception.GraphRuntimeException;
import org.apache.usergrid.persistence.graph.serialization.impl.shard.NodeType;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.CompositeBuilder;
import com.netflix.astyanax.model.CompositeParser;
import com.netflix.astyanax.serializers.LongSerializer;



@Singleton
public class NodeShardCounterSerializationImpl implements NodeShardCounterSerialization {


    private static final ShardKeySerializer SHARD_KEY_SERIALIZER = new ShardKeySerializer();

    /**
     * Edge shards
     */
    private static final MultiTennantColumnFamily<ApplicationScope, ShardKey, Long> EDGE_SHARD_COUNTS =
            new MultiTennantColumnFamily<>( "Edge_Shard_Counts",
                    new OrganizationScopedRowKeySerializer<>( SHARD_KEY_SERIALIZER ), LongSerializer.get() );


    protected final Keyspace keyspace;
    protected final CassandraConfig cassandraConfig;
    protected final GraphFig graphFig;


    @Inject
    public NodeShardCounterSerializationImpl( final Keyspace keyspace, final CassandraConfig cassandraConfig,
                                            final GraphFig graphFig ) {
        this.keyspace = keyspace;
        this.cassandraConfig = cassandraConfig;
        this.graphFig = graphFig;
    }


    @Override
    public MutationBatch flush( final Counter counter ) {


        Preconditions.checkNotNull( counter, "counter must be specified" );


        final MutationBatch batch = keyspace.prepareMutationBatch();

        for ( Map.Entry<ShardKey, AtomicLong> entry : counter.getEntries() ) {

            final ShardKey key = entry.getKey();
            final long value = entry.getValue().get();


            final ScopedRowKey rowKey = ScopedRowKey.fromKey( key.scope, key );


            batch.withRow( EDGE_SHARD_COUNTS, rowKey ).incrementCounterColumn( key.shardId, value );
        }


        return batch;
    }


    @Override
    public long getCount( final ShardKey key ) {

        final ScopedRowKey rowKey = ScopedRowKey.fromKey( key.scope, key );


        try {
            OperationResult<Column<Long>> column =
                    keyspace.prepareQuery( EDGE_SHARD_COUNTS ).getKey( rowKey ).getColumn( key.shardId ).execute();

            return column.getResult().getLongValue();
        }
        //column not found, return 0
        catch ( NotFoundException nfe ) {
            return 0;
        }
        catch ( ConnectionException e ) {
            throw new GraphRuntimeException( "An error occurred connecting to cassandra", e );
        }
    }


    @Override
    public Collection<MultiTennantColumnFamilyDefinition> getColumnFamilies() {
        return Collections.singleton(
                new MultiTennantColumnFamilyDefinition( EDGE_SHARD_COUNTS, BytesType.class.getSimpleName(),
                        ColumnTypes.LONG_TYPE_REVERSED, CounterColumnType.class.getSimpleName(),
                        MultiTennantColumnFamilyDefinition.CacheOption.KEYS ) );
    }



    private static class ShardKeySerializer implements CompositeFieldSerializer<ShardKey> {

        private static final IdRowCompositeSerializer ID_SER = IdRowCompositeSerializer.get();


        @Override
        public void toComposite( final CompositeBuilder builder, final ShardKey key ) {

            ID_SER.toComposite( builder, key.nodeId );

            builder.addInteger( getValue( key.nodeType ) );

            builder.addLong( key.shardId );

            builder.addInteger( key.edgeTypes.length );

            for ( String type : key.edgeTypes ) {
                builder.addString( type );
            }
        }


        @Override
        public ShardKey fromComposite( final CompositeParser composite ) {

            final Id sourceId = ID_SER.fromComposite( composite );

            final NodeType type = getType( composite.readInteger() );

            final long shardId = composite.readLong();

            final int length = composite.readInteger();

            String[] types = new String[length];

            for ( int i = 0; i < length; i++ ) {
                types[i] = composite.readString();
            }

            return new ShardKey(null, sourceId, type, shardId, types);
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

}
