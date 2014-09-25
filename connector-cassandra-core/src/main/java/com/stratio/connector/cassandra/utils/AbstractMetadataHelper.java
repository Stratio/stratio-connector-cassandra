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

package com.stratio.connector.cassandra.utils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.stratio.meta.common.metadata.structures.*;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractMetadataHelper {

    /**
     * Database type mappings.
     */
    protected static Map<ColumnType, String> dbType = new HashMap<>();

    /**
     * Database Java class mappings.
     */
    protected static Map<ColumnType, Class<?>> dbClass = new HashMap<>();





    /**
     * Obtain the ColumnType associated with a database type.
     *
     * @param dbTypeName The name of the database type.
     * @return A {@link com.stratio.meta2.common.metadata.ColumnType}.
     */
    public abstract ColumnType toColumnType(String dbTypeName);


}
