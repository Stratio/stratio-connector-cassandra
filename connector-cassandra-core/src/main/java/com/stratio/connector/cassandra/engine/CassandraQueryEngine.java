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
import com.stratio.meta.common.utils.StringUtils;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class CassandraQueryEngine implements IQueryEngine {
    private List<ColumnName> selectionClause;
    private boolean catalogInc;
    private String catalog;
    private TableName tableName;

    private boolean whereInc = false;
    private boolean orderInc = false;
    private boolean limitInc = false;

    private List<Relation> where = new ArrayList<Relation>();
    private int limit = 0;
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
            for (ColumnName columnName : selectionClause) {
                sb.append(columnName.getName());
                sb.append(",");
            }
            sb.delete(sb.lastIndexOf(","), sb.indexOf(",")+1);
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

                String identifier=relation.getIdentifier().toString();
                String realIdentifier=identifier.substring(identifier.lastIndexOf(".")+1,identifier.length());
                sb.append(realIdentifier);
                sb.append(" " + relation.getOperator().toString() + " ");
                relation.getRightSelectors().get(0).getType();

                //sb.append("'" + relation.getTerms().get(0).toString() + "'");
            }
        }
        if (limitInc) {
            sb.append(" LIMIT ").append(limit);
        }

        return sb.toString().replace("  ", " ");
    }


}
