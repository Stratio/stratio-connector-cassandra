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

import java.util.List;

import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * Class that models a {@code SELECT} statement from the META language. This class recognizes the
 * following syntax:
 * <p/>
 * DELETE ( {@literal <column>}, ( ',' {@literal <column>} )*)? FROM {@literal <tablename>} WHERE
 * {@literal <where_clause>};
 */
public class DeleteStatement {

    private String catalog;
    private boolean catalogInc = false;
    /**
     * The name of the targe table.
     */
    private TableName tableName = null;

    /**
     * The list of {@link com.stratio.crossdata.common.statements.structures.Relation} found
     * in the WHERE clause.
     */
    private List<Filter> whereClauses;

    /**
     * Constructor Class.
     * @param tableName The table Name
     * @param whereClauses A list with the conditions
     */
    public DeleteStatement(TableName tableName, List<Filter> whereClauses) {
        this.tableName = tableName;
        if (tableName.isCompletedName()) {
            catalogInc = true;
            catalog = tableName.getCatalogName().getName();
        }
        this.whereClauses = whereClauses;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        if (catalogInc) {
            sb.append(Utils.toCaseSensitive(catalog)).append(".");
        }
        sb.append(Utils.toCaseSensitive(tableName.getName()));
        if (!whereClauses.isEmpty()) {
            sb.append(" WHERE ");
            for (Filter filter : whereClauses) {
                Relation relation = filter.getRelation();
                String leftTerm = relation.getLeftTerm().getStringValue().substring(relation.getLeftTerm()
                        .getStringValue().lastIndexOf('.') + 1, relation.getLeftTerm().getStringValue().length());
                sb.append(Utils.toCaseSensitive(leftTerm)).append(relation.getOperator().toString()).append
                        (relation.getRightTerm().toString()).append(" AND ");
            }
            sb.delete(sb.lastIndexOf(" AND"), sb.length());

        }
        return sb.toString();
    }

    public TableName getTableName() {
        return tableName;
    }

    /**
     * Set the name of the table.
     *
     * @param tableName The name of the table.
     */
    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

}
