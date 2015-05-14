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
 * Class that models a {@code DROP KEYSPACE} statement from the META language.
 */
public class DropCatalogStatement {
    /**
     * Catalog.
     */
    private String catalog;

    /**
     * Whether the keyspace should be removed only if exists.
     */
    private boolean ifExists;

    /**
     * Class constructor.
     *
     * @param catalog  The name of the catalog.
     * @param ifExists Whether it should be removed only if exists.
     */
    public DropCatalogStatement(String catalog, boolean ifExists) {

        this.catalog = catalog;
        this.ifExists = ifExists;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DROP KEYSPACE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(Utils.toCaseSensitive(catalog));
        return sb.toString();
    }

}
