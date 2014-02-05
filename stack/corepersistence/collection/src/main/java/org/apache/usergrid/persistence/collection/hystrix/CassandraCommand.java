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

package org.apache.usergrid.persistence.collection.hystrix;


import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;

import rx.Observable;


/**
 * Default command that just returns the value handed to it.  Useful for creating observables
 * that are subscribed on the correct underlying Hystrix thread pool
 *
 */
public class CassandraCommand<R> extends HystrixCommand<R> {

    private static final HystrixCommandGroupKey GROUP_KEY =HystrixCommandGroupKey.Factory.asKey( "CassandraCommand" );


    private final R value;

    protected CassandraCommand(final R value) {
        super( GROUP_KEY );
        this.value = value;
    }


    @Override
    protected R run() throws Exception {
        return value;
    }


    /**
     * Get the write command
     *
     * @param readValue The value to observe on
     * @return The value wrapped in a Hystrix observable
     */
    public static <R> Observable<R> toObservable( R readValue ){
         return new CassandraCommand<R>(readValue).toObservable();
    }

}