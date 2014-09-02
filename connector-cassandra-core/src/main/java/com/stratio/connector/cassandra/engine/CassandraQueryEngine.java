/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.cassandra.engine;


import com.datastax.driver.core.Session;

import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.*;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class CassandraQueryEngine implements IQueryEngine {
    private List<ColumnName> selectionClause;
    private boolean catalogInc;
    private String catalog;
    private TableName tableName;



    private boolean whereInc = false;
    private boolean orderInc = false;
    private boolean limitInc = false;

    private List<Relation> where = new ArrayList<Relation>();
    private int limit = 100;
    private Map<String, Session> sessions;

    public CassandraQueryEngine(Map<String, Session> sessions) {
        this.sessions = sessions;
    }

    @Override
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow)
        throws UnsupportedException, ExecutionException {
        Session session = sessions.get(targetCluster.getName());

        if (workflow.getInitialSteps().size() > 1) {
            throw new UnsupportedException("");
        } else {
            LogicalStep logicalStep = workflow.getInitialSteps().get(0);
            while (logicalStep != null) {
                if (logicalStep instanceof TransformationStep) {
                    TransformationStep transformation = (TransformationStep) logicalStep;
                    if (transformation instanceof Project) {
                        Project project = (Project) transformation;

                        tableName = project.getTableName();

                        catalogInc = tableName.isCompletedName();
                        if (catalogInc) {
                            CatalogName catalogName = tableName.getCatalogName();
                            catalog = catalogName.getName();
                        }
                        selectionClause = project.getColumnList();
                    } else {
                        if (transformation instanceof Filter) {
                            Filter filter = (Filter) transformation;
                            whereInc = true;
                            Relation relation = filter.getRelation();
                            where.add(relation);
                        }
                    }
                }
                logicalStep = logicalStep.getNextStep();
            }
        }
        String query = parseQuery();
        QueryResult result =QueryResult.createQueryResult(CassandraExecutor.execute(query, session));
        return result;
    }

    public String parseQuery() {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (selectionClause != null) {
            int i=0;
            for (ColumnName columnName : selectionClause) {
                if (i!=0)
                    sb.append(",");
                i=1;
                sb.append(columnName.getName());
            }

        }
        sb.append(" FROM ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName.getName());

        if (whereInc) {
            sb.append(" WHERE ");
            int count=0;
            for(Relation relation:where) {
                if (count>0)
                    sb.append(" AND ");
                count=1;

                //TODO adjust to new META Select refactor
                //DIFRENT CASES: IN, BETWEEN, MATCH, OTHERS

                switch (relation.getOperator()){
                    case IN:
                    case BETWEEN:
                        break;
                    case MATCH:
                        sb.append(getLuceneWhereClause(relation));
                        break;
                    default:

                        sb.append(relation.toString());
                        break;
                }
            }
        }
        if (limitInc) {
            sb.append(" LIMIT ").append(limit);
        }
        return sb.toString().replace("  ", " ");
    }

    public String[] getLuceneWhereClause(Relation relation) {
        String[] result;

        StringBuilder sb = new StringBuilder("{filter:{type:\"boolean\",must:[");


        String column = relation.getLeftTerm().toString();
        String value = relation.getRightTerm().toString();
        // Generate query for column
        String[] processedQuery = processLuceneQueryType(value);
        sb.append("{type:\"");
        sb.append(processedQuery[0]);
        sb.append("\",field:\"");
        sb.append(column);
        sb.append("\",value:\"");
        sb.append(processedQuery[1]);
        sb.append("\"},");

        sb.replace(sb.length() - 1, sb.length(), "");
        sb.append("]}}");

        result = new String[] {sb.toString()};

    return result;

    }



    /**
     * Process a query pattern to determine the type of Lucene query. The supported types of queries
     * are: <li>
     * <ul>
     * Wildcard: The query contains * or ?.
     * </ul>
     * <ul>
     * Fuzzy: The query ends with ~ and a number.
     * </ul>
     * <ul>
     * Regex: The query contains [ or ].
     * </ul>
     * <ul>
     * Match: Default query, supporting escaped symbols: *, ?, [, ], etc.
     * </ul>
     * </li>
     *
     * @param query The user query.
     * @return An array with the type of query and the processed query.
     */
    protected String[] processLuceneQueryType(String query) {
        String[] result = {"", ""};
        Pattern escaped = Pattern.compile(".*\\\\\\*.*|.*\\\\\\?.*|.*\\\\\\[.*|.*\\\\\\].*");
        Pattern wildcard = Pattern.compile(".*\\*.*|.*\\?.*");
        Pattern regex = Pattern.compile(".*\\].*|.*\\[.*");
        Pattern fuzzy = Pattern.compile(".*~\\d+");
        if (escaped.matcher(query).matches()) {
            result[0] = "match";
            result[1] =
                query.replace("\\*", "*").replace("\\?", "?").replace("\\]", "]").replace("\\[", "[");
        } else if (regex.matcher(query).matches()) {
            result[0] = "regex";
            result[1] = query;
        } else if (fuzzy.matcher(query).matches()) {
            result[0] = "fuzzy";
            result[1] = query;
        } else if (wildcard.matcher(query).matches()) {
            result[0] = "wildcard";
            result[1] = query;
        } else {
            result[0] = "match";
            result[1] = query;
        }
        // C* Query builder doubles the ' character.
        result[1] = result[1].replaceAll("^'", "").replaceAll("'$", "");
        return result;
    }





}
