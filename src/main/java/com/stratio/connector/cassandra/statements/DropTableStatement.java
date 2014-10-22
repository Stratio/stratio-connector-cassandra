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

/**
 * Class that models a {@code DROP TABLE} statement from the META language.
 */
public class DropTableStatement {
    /**
     * Catalog
     */
    private String catalog;

    /**
     * Indicates if there is a catalog specified in the table name
     */
    private boolean catalogInc;
    /**
     * The name of the target table.
     */
    private String tableName;

    /**
     * Whether the table should be dropped only if exists.
     */
    private boolean ifExists;

    /**
     * Class constructor.
     *
     * @param tableName The name of the table.
     * @param ifExists  Whether it should be dropped only if exists.
     */
    public DropTableStatement(String tableName, boolean ifExists) {
        if (tableName.contains(".")) {
            String[] ksAndTableName = tableName.split("\\.");
            catalog = ksAndTableName[0];
            this.tableName = ksAndTableName[1];
            catalogInc = true;
        } else {
            this.tableName = tableName;
        }
        this.ifExists = ifExists;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName);
        return sb.toString();
    }

}
