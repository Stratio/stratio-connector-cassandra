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


import com.stratio.connector.cassandra.utils.ColumnInsertCassandra;
import com.stratio.meta.common.utils.StringUtils;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Class that models an {@code INSERT INTO} statement from the META language.
 */
public class InsertIntoStatement {


    /**
     * The name of the target table.
     */
    private String tableName;

    /**
     * The list of columns to be assigned.
     */
    private List<String> ids;



    private Map<String, ColumnInsertCassandra> cellValues;

    /**
     * Indicates if exists "IF NOT EXISTS" clause.
     */
    private boolean ifNotExists;


    /**
     * Catalog
     */
    private String catalog;

    /**
     * Indicates if there is a catalog specified in the table name
     */
    private boolean catalogInc;



    /**
     * InsertIntoStatement general constructor.
     *
     * @param targetTable     Table target.
     * @param columnsMetadata List of {@link com.stratio.connector.cassandra.utils.ColumnInsertCassandra} to insert.
     * @param ifNotExists     Boolean that indicates if IF NOT EXISTS clause is included in the query.
     */
    public InsertIntoStatement(TableMetadata targetTable,
        Map<String, ColumnInsertCassandra> columnsMetadata,
        boolean ifNotExists) {
        ids = new ArrayList<>();
        this.tableName = targetTable.getName().getQualifiedName();
        if (tableName.contains(".")) {
            String[] ksAndTableName = tableName.split("\\.");
            catalog = ksAndTableName[0];
            this.tableName = ksAndTableName[1];
            catalogInc = true;
        }

        for (String id : columnsMetadata.keySet()) {
            ids.add(id);
        }
        this.cellValues = columnsMetadata;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName).append(" (");
        sb.append(StringUtils.stringList(ids, ", ")).append(") ");

        sb.append("VALUES (");

        int cont = 0;
        for (String column : cellValues.keySet()) {
            String value = cellValues.get(column).getValue();
            ColumnType type = cellValues.get(column).getType();
            if (cont > 0) {
                sb.append(", ");
            }
            cont = 1;

            switch (type) {
                case TEXT:
                case VARCHAR:
                    sb.append("'" + value + "'");
                    break;
                default:
                    sb.append(value);
                    break;
            }
        }
        sb.append(")");

        if (ifNotExists) {
            sb.append(" IF NOT EXISTS");
        }

        return sb.toString();
    }



}
