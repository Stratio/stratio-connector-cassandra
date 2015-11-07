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

package com.stratio.connector.cassandra.statements;

import com.stratio.connector.cassandra.utils.Utils;

/**
 * Class that models a {@code CREATE CATALOG} statement from the META language. Catalog
 * information will be stored internally as part of the existing metadata. Catalog creation
 * in the underlying datastore is done when a table is created in a catalog.
 */
public class CreateCatalogStatement {
    /**
     * Catalog.
     */
    private String catalog;

    /**
     * Whether the keyspace should be created only if it not exists.
     */
    private final boolean ifNotExists;

    /**
     * A JSON with the options specified by the user.
     */
    private final String options;

    /**
     * Class constructor.
     *
     * @param catalogName The name of the catalog.
     * @param ifNotExists Whether it should be created only if it not exists.
     * @param options     A JSON with the storage options.
     */
    public CreateCatalogStatement(String catalogName, boolean ifNotExists,
            String options) {
        this.catalog = catalogName;
        this.ifNotExists = ifNotExists;
        this.options = options;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(Utils.toCaseSensitive(catalog));

        sb.append(" WITH ").append(options);

        if ((options != null) && (options.length() > 0)) {
            sb.append(options);
        } else {
            sb.append("replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
        }
        return sb.toString();
    }

}

