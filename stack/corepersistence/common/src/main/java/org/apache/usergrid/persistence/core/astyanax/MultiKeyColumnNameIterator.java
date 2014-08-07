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
package org.apache.usergrid.persistence.core.astyanax;


import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Observable;
import java.util.TreeMap;
import java.util.TreeSet;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.hystrix.HystrixCommandGroupKey;


/**
 * Simple iterator that wraps a collection of ColumnNameIterators.  We do this because we can't page with a multiRangeScan
 * correctly for multiple trips.  As a result, we do this since only 1 iterator with minimum values could potentially
 * feed the entire result set.
 *
 * Compares the parsed values and puts them in order. If more than one row key emits the same value
 * the first value is selected, and ignored from subsequent iterators.
 *
 */
public class MultiKeyColumnNameIterator<C, T> implements Iterable<T>, Iterator<T> {


    private static final HystrixCommandGroupKey GROUP_KEY = HystrixCommandGroupKey.Factory.asKey( "CassRead" );


   private final Comparator<T> comparator;


    public MultiKeyColumnNameIterator(Collection<ColumnNameIterator<C, T>> columnNameIterators, final Comparator<T> comparator) {

        this.comparator = comparator;


        advanceIterator();
    }


    @Override
    public Iterator<T> iterator() {
        return this;
    }


    @Override
    public boolean hasNext() {
        //if we've exhausted this iterator, try to advance to the next set
        if ( sourceIterator.hasNext() ) {
            return true;
        }

        //advance the iterator, to the next page, there could be more
        advanceIterator();

        return sourceIterator.hasNext();
    }


    @Override
    public T next() {

        if ( !hasNext() ) {
            throw new NoSuchElementException();
        }

        return parser.parseColumn( sourceIterator.next() );
    }


    @Override
    public void remove() {
        sourceIterator.remove();
    }


    /**
     * Execute the query again and set the reuslts
     */
    private void advanceIterator() {

        //run producing the values within a hystrix command.  This way we'll time out if the read takes too long
        try {
            sourceIterator = rowQuery.execute().getResult().iterator();
        }
        catch ( ConnectionException e ) {
            throw new RuntimeException( "Unable to get next page", e );
        }
    }


    private final class MultiColumIteratorSet{

        private TreeMap<T, ColumnNameIterator<C, T>> sources;




        public boolean isInitialized(){
            return sources != null;
        }



        public boolean hasNext(){
            return sources == null && sources.size() > 0;
        }


        public void initialize(Collection<ColumnNameIterator<C, T>> columnNameIterators){



            for (ColumnNameIterator)

        }




    }



}
