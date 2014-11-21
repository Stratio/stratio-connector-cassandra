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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * Class that models an {@code UPDATE} statement from the META language.
 */
public class UpdateTableStatement {

    /**
     * The name of the table.
     */
    private TableName tableName;

    /**
     * The list of assignations.
     */
    private List<Relation> assignations = new ArrayList<>();

    /**
     * The list of relations.
     */
    private List<Filter> whereClauses = new ArrayList<>();

    /**
     * Class constructor.
     *
     * @param tableName    The name of the table.
     * @param assignations The list of assignations.
     * @param whereClauses The list of relations.
     */
    public UpdateTableStatement(TableName tableName, Collection<Relation> assignations,
            Collection<Filter> whereClauses) {

        for (Filter filter : whereClauses) {
            this.whereClauses.add(filter);
        }

        for (Relation relation : assignations) {
            this.assignations.add(relation);
        }

        this.tableName = tableName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(tableName.getQualifiedName());

        sb.append(" ").append("SET ");
        for (Relation relation : assignations) {
            String leftTerm = getLeftTerm(relation);
            sb.append(leftTerm).append(relation.getOperator().toString()).append
                    (relation.getRightTerm().toString()).append(", ");
        }

        sb.delete(sb.lastIndexOf(", "), sb.length());

        sb.append(" WHERE ");
        if ((whereClauses != null) && (!whereClauses.isEmpty())) {
            for (Filter filter : whereClauses) {
                Relation relation = filter.getRelation();
                String leftTerm = getLeftTerm(relation);
                sb.append(leftTerm).append(relation.getOperator().toString()).append
                        (relation.getRightTerm().toString()).append(" AND ");
            }
            sb.delete(sb.lastIndexOf(" AND"), sb.length());
        }

        return sb.toString();
    }

    private String getLeftTerm(Relation relation){
        String leftTerm = relation.getLeftTerm().getStringValue().substring(relation.getLeftTerm()
                .getStringValue().lastIndexOf('.') + 1, relation.getLeftTerm().getStringValue().length());

        return leftTerm;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

}
