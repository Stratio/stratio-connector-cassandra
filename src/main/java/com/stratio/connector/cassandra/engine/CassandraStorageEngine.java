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

package com.stratio.connector.cassandra.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.connector.cassandra.statements.DeleteStatement;
import com.stratio.connector.cassandra.statements.InsertIntoStatement;
import com.stratio.connector.cassandra.statements.TruncateStatement;
import com.stratio.connector.cassandra.statements.UpdateTableStatement;
import com.stratio.connector.cassandra.utils.ColumnInsertCassandra;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * Class CassandraStorageEngine: Allow to make insert queries with the connector.
 */
public class CassandraStorageEngine implements IStorageEngine {
    private Map<String, Session> sessions;

    /**
     * Basic Constructor.
     *
     * @param sessions Map with the sessions
     */
    public CassandraStorageEngine(Map<String, Session> sessions) {
        this.sessions = sessions;
    }

    /**
     * Insert method to a table.
     *
     * @param targetCluster The target cluster.
     * @param targetTable   The target table.
     * @param row           The inserted row.
     * @throws ConnectorException
     */
    @Override
    public void insert(ClusterName targetCluster,
            com.stratio.crossdata.common.metadata.TableMetadata targetTable, Row row, boolean ifNotExists)
            throws ConnectorException {
        Session session = sessions.get(targetCluster.getName());
        String query = insertBlock(row, targetTable, false);
        CassandraExecutor.execute(query, session);
    }

    /**
     * Multiple insertion in a table.
     *
     * @param targetCluster The target cluster.
     * @param targetTable   The target table.
     * @param rows          Collection of rows to insert.
     * @throws ConnectorException
     */
    @Override
    public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows, boolean ifNotExists)
            throws ConnectorException {
        Session session = sessions.get(targetCluster.getName());
        for (Row row : rows) {
            String query = insertBlock(row, targetTable, false);
            CassandraExecutor.execute(query, session);
        }
    }

    private String insertBlock(Row row, TableMetadata targetTable, boolean ifNotExists) throws ExecutionException {
        Set<String> keys = row.getCells().keySet();
        Map<ColumnName, ColumnMetadata> columnsWithMetadata = targetTable.getColumns();
        Map<String, ColumnInsertCassandra> columnsMetadata = new HashMap<>();
        try {
            for (String key : keys) {
                ColumnName col =
                        new ColumnName(targetTable.getName().getCatalogName().getName(),
                                targetTable.getName().getName(), key);
                columnsMetadata.put(key,
                        new ColumnInsertCassandra(columnsWithMetadata.get(col).getColumnType(),
                                row.getCell(key).toString(), key));
            }
        } catch (Exception e) {
            throw new ExecutionException("Trying insert data in a not existing column", e);
        }

        InsertIntoStatement insertStatement =
                new InsertIntoStatement(targetTable, columnsMetadata, ifNotExists);
        return insertStatement.toString();
    }

    @Override
    public void delete(ClusterName targetCluster, TableName tableName, Collection<Filter> whereClauses)
            throws ConnectorException {
        Session session = sessions.get(targetCluster.getName());
        List<Filter> whereFilters = new ArrayList<>();
        for (Filter filter : whereClauses) {
            whereFilters.add(filter);
        }
        DeleteStatement deleteStatement = new DeleteStatement(tableName, whereFilters);
        String query = deleteStatement.toString();
        CassandraExecutor.execute(query, session);
    }

    @Override
    public void update(ClusterName targetCluster, TableName tableName, Collection<Relation> assignments,
            Collection<Filter> whereClauses) throws ConnectorException {
        Session session = sessions.get(targetCluster.getName());
        UpdateTableStatement updateStatement = new UpdateTableStatement(tableName, assignments, whereClauses);
        String query = updateStatement.toString();
        CassandraExecutor.execute(query, session);
    }

    @Override
    public void truncate(ClusterName targetCluster, TableName tableName) throws ConnectorException {
        Session session = sessions.get(targetCluster.getName());
        TruncateStatement truncateStatement = new TruncateStatement(tableName);
        String query = truncateStatement.toString();
        CassandraExecutor.execute(query, session);
    }

}
