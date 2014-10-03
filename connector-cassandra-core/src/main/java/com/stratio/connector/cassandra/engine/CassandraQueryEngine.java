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


import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.CriticalExecutionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.*;
import com.stratio.meta.common.result.ErrorResult;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.result.Result;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class CassandraQueryEngine implements IQueryEngine {
    Map<ColumnName, String> aliasColumns = new HashMap<>();
    Session session = null;
    private List<ColumnName> selectionClause;
    private boolean catalogInc;
    private String catalog;
    private TableName tableName;
    private boolean whereInc = false;
    private boolean limitInc = true;
    private List<Relation> where = new ArrayList<>();
    private int limit = 100;
    private Map<String, Session> sessions;

    public CassandraQueryEngine(Map<String, Session> sessions, int limitDefault) {
        this.sessions = sessions;
        this.limit = limitDefault;
    }

    @Override
    public com.stratio.meta.common.result.QueryResult execute(LogicalWorkflow workflow)
        throws UnsupportedException, ExecutionException {

        LogicalStep logicalStep = workflow.getInitialSteps().get(0);
        while (logicalStep != null) {
            if (logicalStep instanceof TransformationStep) {
                getTransformationStep(logicalStep);
            }
            logicalStep = logicalStep.getNextStep();
        }

        String query = parseQuery();

        Result result;
        if (session != null) {
            if (aliasColumns.isEmpty()) {
                result = CassandraExecutor.execute(query, session);
            } else {
                result = CassandraExecutor.execute(query, aliasColumns, session);
            }
        } else {
            throw new ExecutionException("No session to cluster established");
        }

        if (result.hasError()) {
            ErrorResult error = (ErrorResult) result;
            getTypeErrorException(error);
        }

        return (QueryResult) result;

    }

    private void getTypeErrorException(ErrorResult error)
        throws ExecutionException, UnsupportedException {
        switch (error.getType()) {
            case EXECUTION:
                throw new ExecutionException(error.getErrorMessage());
            case NOT_SUPPORTED:
                throw new UnsupportedException(error.getErrorMessage());
            case CRITICAL:
                throw new CriticalExecutionException(error.getErrorMessage());
            default:
                throw new UnsupportedException(error.getErrorMessage());
        }
    }

    private void getTransformationStep(LogicalStep logicalStep) {
        TransformationStep transformation = (TransformationStep) logicalStep;
        if (transformation instanceof Project) {
            Project project = (Project) transformation;
            session = sessions.get(project.getClusterName().getName());

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
            } else if (transformation instanceof Limit) {
                Limit limitClause = (Limit) transformation;
                limit = limitClause.getLimit();
            } else if (transformation instanceof Select) {
                Select select = (Select) transformation;
                aliasColumns = select.getColumnMap();
            }
        }
    }

    @Override public void asyncExecute(String queryId, LogicalWorkflow workflow,
        IResultHandler resultHandler) throws UnsupportedException, ExecutionException {

    }

    @Override public void stop(String queryId) throws UnsupportedException, ExecutionException {

    }



    public String parseQuery() {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (selectionClause != null) {
            sb.append(getSelectionClause());
        }
        sb.append(getFromClause());

        if (whereInc) {
            sb.append(getWhereClause());
        }

        if (limitInc) {
            sb.append(" LIMIT ").append(limit);
        }
        return sb.toString().replace("  ", " ");
    }

    private String getWhereClause() {
        StringBuilder sb=new StringBuilder();
        sb.append(" WHERE ");
        int count = 0;
        for (Relation relation : where) {
            if (count > 0) {
                sb.append(" AND ");
            }
            count = 1;
            switch (relation.getOperator()) {
                case IN:
                case BETWEEN:
                    break;
                case MATCH:
                    String nameIndex = getLuceneIndex();
                    sb.append(nameIndex).append(" = '");
                    sb.append(getLuceneWhereClause(relation));
                    sb.append("'");
                    break;
                default:
                    String whereWithQualification = relation.toString();
                    String parts[] = whereWithQualification.split(" ");
                    String columnName = parts[0].substring(parts[0].lastIndexOf(".") + 1);
                    sb.append(columnName);
                    for (int i = 1; i < parts.length; i++) {
                        sb.append(" ").append(parts[i]);
                    }
                    break;
            }
        }

        return sb.toString();

    }

    private String getFromClause() {
        StringBuilder sb=new StringBuilder();
        sb.append(" FROM ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName.getName());
        return sb.toString();
    }

    private String getSelectionClause() {
        StringBuilder sb=new StringBuilder();
        int i = 0;
        for (ColumnName columnName : selectionClause) {
            if (i != 0) {
                sb.append(",");
            }
            i = 1;
            sb.append(columnName.getName());
        }
        return sb.toString();
    }

    private String getLuceneIndex() {
        String indexName = "";
        List<ColumnMetadata> columns =
            session.getCluster().getMetadata().getKeyspace(catalog).getTable(tableName.getName())
                .getColumns();
        for (ColumnMetadata column : columns) {

            if (column.getIndex() != null) {
                if (column.getIndex().isCustomIndex()) {
                    indexName = column.getIndex().getName();
                }
            }

        }
        return indexName;
    }

    public String getLuceneWhereClause(Relation relation) {
        String result;

        StringBuilder sb = new StringBuilder("{filter:{type:\"boolean\",must:[");


        String column = relation.getLeftTerm().toString()
            .substring(relation.getLeftTerm().toString().lastIndexOf('.') + 1);
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

        result = sb.toString();

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
                query.replace("\\*", "*").replace("\\?", "?").replace("\\]", "]")
                    .replace("\\[", "[");
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
