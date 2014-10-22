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
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.connector.cassandra.statements.CreateCatalogStatement;
import com.stratio.connector.cassandra.statements.CreateIndexStatement;
import com.stratio.connector.cassandra.statements.CreateTableStatement;
import com.stratio.connector.cassandra.statements.DropCatalogStatement;
import com.stratio.connector.cassandra.statements.DropIndexStatement;
import com.stratio.connector.cassandra.statements.DropTableStatement;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.exceptions.CriticalExecutionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.ErrorResult;
import com.stratio.crossdata.common.result.Result;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;
import com.stratio.crossdata.common.statements.structures.selectors.StringSelector;

public class CassandraMetadataEngine implements IMetadataEngine {

    private static final int PRIMARY_SINGLE = 1;
    private static final int PRIMARY_COMPOSED = 2;
    private static final int PRIMARY_AND_CLUSTERING_SPECIFIED = 3;
    private static List<String> createTableOptions = new ArrayList<>();

    static {
        createTableOptions.add("bloom_filter_fp_chance");
        createTableOptions.add("comment");
        createTableOptions.add("dclocal_read_repair_chance");
        createTableOptions.add("default_time_to_live");
        createTableOptions.add("gc_grace_seconds");
        createTableOptions.add("min_index_interval");
        createTableOptions.add("max_index_interval");
        createTableOptions.add("min_index_interval");
        createTableOptions.add("populate_io_cache_on_flush");
        createTableOptions.add("read_repair_chance");
        createTableOptions.add("speculative_retry");
        createTableOptions.add("durable_writes");
        createTableOptions.add("populate_io_cache_on_flush");

    }

    private Map<String, Session> sessions;
    private Session session = null;

    public CassandraMetadataEngine(Map<String, Session> sessions) {
        this.sessions = sessions;
    }

    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
            throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());

        String catalogName = catalogMetadata.getName().getQualifiedName();

        Map<Selector, Selector> catalogOptions = catalogMetadata.getOptions();

        String stringOptions = getStringOptions(catalogOptions);

        CreateCatalogStatement catalogStatement =
                new CreateCatalogStatement(catalogName, true, stringOptions);

        Result result = CassandraExecutor.execute(catalogStatement.toString(), session);

        checkError(result);

    }

    @Override
    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata)
            throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());

        Map<Selector, Selector> tableOptions = tableMetadata.getOptions();
        List<ColumnName> primaryKey = tableMetadata.getPrimaryKey();
        List<ColumnName> partitionKey = tableMetadata.getPartitionKey();
        List<ColumnName> clusterKey = tableMetadata.getClusterKey();

        int primaryKeyType;
        if (primaryKey.size() == 1) {
            primaryKeyType = PRIMARY_SINGLE;
        } else {
            if (clusterKey.isEmpty()) {
                primaryKeyType = PRIMARY_AND_CLUSTERING_SPECIFIED;
            } else {
                primaryKeyType = PRIMARY_COMPOSED;
            }
        }
        String stringOptions = getStringOptions(tableOptions);

        CreateTableStatement tableStatement =
                new CreateTableStatement(tableMetadata, primaryKey, partitionKey, clusterKey,
                        primaryKeyType, stringOptions, true);
        Result result = CassandraExecutor.execute(tableStatement.toString(), session);
        checkError(result);

    }

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName name)
            throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        DropCatalogStatement catalogStatement = new DropCatalogStatement(name.getName(), true);
        Result result = CassandraExecutor.execute(catalogStatement.toString(), session);
        checkError(result);
    }

    @Override
    public void dropTable(ClusterName targetCluster, TableName name)
            throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        DropTableStatement tableStatement = new DropTableStatement(name.getQualifiedName(), true);
        Result result = CassandraExecutor.execute(tableStatement.toString(), session);
        checkError(result);

    }

    @Override
    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
            throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        CreateIndexStatement indexStatement =
                new CreateIndexStatement(indexMetadata, true, session);
        Result result = CassandraExecutor.execute(indexStatement.toString(), session);
        checkError(result);
    }

    @Override
    public void dropIndex(ClusterName targetCluster, IndexMetadata indexName)
            throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        DropIndexStatement indexStatement = new DropIndexStatement(indexName, true);
        Result result = CassandraExecutor.execute(indexStatement.toString(), session);
        checkError(result);

    }

    private String getStringOptions(Map<Selector, Selector> options) {
        StringBuilder stringOptions = new StringBuilder();
        if ((options != null) && (!options.isEmpty())) {
            int i = 0;

            for (Map.Entry<Selector, Selector> entry : options.entrySet()) {
                StringSelector stringKeySelector = (StringSelector) entry.getKey();
                StringSelector optionSelector = (StringSelector) entry.getValue();

                if (i != 0) {
                    stringOptions.append(" AND ");
                }
                i = 1;
                //Analize if it is a pair {key,value} or its only a key
                String key = stringKeySelector.getValue();
                stringOptions.append(getStyleStringOption(key, optionSelector.getValue()));
            }
        }
        return stringOptions.toString();
    }

    private StringBuilder getStyleStringOption(String key, String value) {
        StringBuilder stringOption = new StringBuilder();

        if ("COMPACT STORAGE".equals(key)) {
            stringOption.append(key);
        } else if ("CLUSTERING ORDER BY".equals(key)) {
            stringOption.append(key).append(" (").append(value).append(")");
        } else if (createTableOptions.contains(key)) {
            stringOption.append(key).append(" = ").append(value).append("");
        } else {
            stringOption.append(key).append(" = {")
                    .append(value).append("}");
        }

        return stringOption;
    }

    private void checkError(Result result) throws ExecutionException, UnsupportedException {
        if (result.hasError()) {
            ErrorResult error = (ErrorResult) result;
            getTypeErrorException(error);
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
}
