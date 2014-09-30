/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.cassandra.data;

import com.stratio.meta.common.data.Row;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CResultSetIterator implements Iterator<Row> {

    /**
     * Set representing a result from Cassandra.
     */
    private final CassandraResultSet cResultSet;

    /**
     * Pointer to the current element.
     */
    private int current;

    /**
     * Build a {@link com.stratio.meta.common.data.CResultSetIterator} from a {@link com.stratio.meta.common.data.CassandraResultSet}.
     *
     * @param cResultSet Cassandra Result Set.
     */
    public CResultSetIterator(CassandraResultSet cResultSet) {
        this.cResultSet = cResultSet;
        this.current = 0;
    }

    @Override
    public boolean hasNext() {
        return current < cResultSet.getRows().size();
    }

    @Override
    public com.stratio.meta.common.data.Row next() throws NoSuchElementException {
        return cResultSet.getRows().get(current++);
    }

    @Override
    public void remove() throws UnsupportedOperationException, IllegalStateException {
        cResultSet.remove(current);
    }
}
