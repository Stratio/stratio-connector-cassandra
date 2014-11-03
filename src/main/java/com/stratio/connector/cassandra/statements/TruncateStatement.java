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

import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.TableName;

/**
 * TruncateStatement class.
 */
public class TruncateStatement {

    /**
     * The name of the table.
     */
    private TableName tableName;
    private boolean catalogInc;
    private String catalog;

    /**
     * Class Constructor.
     *
     * @param tableName The table name of the truncate statement.
     */
    public TruncateStatement(TableName tableName) {

        this.tableName = tableName;
        this.catalogInc= tableName.isCompletedName();
        this.catalog= tableName.getCatalogName().getName();
    }

    /**
     * Get the table of the truncate statement.
     *
     * @return com.stratio.crossdata.common.data.TableName
     */
    public TableName getTableName() {
        return tableName;
    }


   /**
     * Get the catalog of the truncate statement.
     *
     * @return com.stratio.crossdata.common.data.CatalogName
     */
    public CatalogName getCatalog() {
        return tableName.getCatalogName();
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TRUNCATE ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName.getName());
        return sb.toString();
    }

}
