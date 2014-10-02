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

import com.stratio.meta2.common.metadata.IndexMetadata;

/**
 * Class that models a {@code DROP KEYSPACE} statement from the META language.
 */
public class DropIndexStatement {
    private String catalog;
    private String indexName;


    /**
     * Indicates if there is a catalog specified in the table name
     */
    private boolean catalogInc;

    /**
     * Whether the keyspace should be removed only if exists.
     */
    private boolean ifExists;

    /**
     * Class constructor.
     *
     * @param index    The name of the index.
     * @param ifExists Whether it should be removed only if exists.
     */
    public DropIndexStatement(IndexMetadata index, boolean ifExists) {



        if (index.getColumns() != null && index.getColumns().size() != 0) {
            this.catalogInc = true;
            this.catalog =
                index.getColumns().get(0).getName().getTableName().getCatalogName().getName();
        } else {
            String[] indexQ = index.getName().getQualifiedName().split(".");
            if (indexQ.length > 1) {
                this.catalog = indexQ[0];
                catalogInc = true;
            } else {
                catalogInc = false;
            }
        }
        this.indexName = index.getName().getName();

        this.ifExists = ifExists;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DROP INDEX ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        if (catalogInc) {
            sb.append(catalog).append(".\"").append(indexName).append("\"");
        } else {
            sb.append("\"").append(indexName).append("\"");
        }

        return sb.toString();
    }


}
