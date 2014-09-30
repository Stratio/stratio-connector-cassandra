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

public abstract class ResultSet implements Iterable<Row> {

    /**
     * Get a Iterator over the ResultSet.
     *
     * @return Iterator
     */
    public abstract Iterator<Row> iterator();

    /**
     * Get the size of ResultSet.
     *
     * @return the size of ResultSet
     */
    public abstract int size();

    /**
     * Check if ResultSet is empty.
     *
     * @return {@code true} if is empty
     */
    public boolean isEmpty() {
        return size() < 1;
    }
}
