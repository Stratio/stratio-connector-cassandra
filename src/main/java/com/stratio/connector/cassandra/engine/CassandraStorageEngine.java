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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.connector.cassandra.statements.InsertIntoStatement;
import com.stratio.connector.cassandra.utils.ColumnInsertCassandra;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.CriticalExecutionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.result.ErrorResult;
import com.stratio.crossdata.common.result.Result;

public class CassandraStorageEngine implements IStorageEngine {
    private Map<String, Session> sessions;

    public CassandraStorageEngine(Map<String, Session> sessions) {
        this.sessions = sessions;
    }

    @Override
    public void insert(ClusterName targetCluster,
            com.stratio.crossdata.common.metadata.TableMetadata targetTable, Row row)
            throws UnsupportedException, ExecutionException {
        Session session = sessions.get(targetCluster.getName());

        Set<String> keys = row.getCells().keySet();

        Map<ColumnName, ColumnMetadata> columnsWithMetadata = targetTable.getColumns();
        Map<String, ColumnInsertCassandra> columnsMetadata = new HashMap<>();

        try {
            for (String key : keys) {
                ColumnName col = new ColumnName(targetTable.getName().getCatalogName().getName(),
                        targetTable.getName().getName(), key);
                columnsMetadata.put(key,
                        new ColumnInsertCassandra(columnsWithMetadata.get(col).getColumnType(),
                                row.getCell(key).toString(), key));
            }
        } catch (Exception e) {
            throw new ExecutionException("Trying insert data in a not existing column", e);
        }

        InsertIntoStatement insertStatement =
                new InsertIntoStatement(targetTable, columnsMetadata, true);
        String query = insertStatement.toString();
        Result result = CassandraExecutor.execute(query, session);

        checkError(result);

    }

    @Override
    public void insert(ClusterName targetCluster,
            com.stratio.crossdata.common.metadata.TableMetadata targetTable, Collection<Row> rows)
            throws UnsupportedException, ExecutionException {
        Session session = sessions.get(targetCluster.getName());
        for (Row row : rows) {
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
                    new InsertIntoStatement(targetTable, columnsMetadata, true);
            String query = insertStatement.toString();
            Result result = CassandraExecutor.execute(query, session);
            checkError(result);
        }
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

    private void checkError(Result result) throws ExecutionException, UnsupportedException {
        if (result.hasError()) {
            ErrorResult error = (ErrorResult) result;
            getTypeErrorException(error);
        }
    }

}