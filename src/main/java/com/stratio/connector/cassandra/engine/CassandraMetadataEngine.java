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
import com.stratio.connector.cassandra.statements.AlterCatalogStatement;
import com.stratio.connector.cassandra.statements.AlterTableStatement;
import com.stratio.connector.cassandra.statements.CreateCatalogStatement;
import com.stratio.connector.cassandra.statements.CreateIndexStatement;
import com.stratio.connector.cassandra.statements.CreateTableStatement;
import com.stratio.connector.cassandra.statements.DropCatalogStatement;
import com.stratio.connector.cassandra.statements.DropIndexStatement;
import com.stratio.connector.cassandra.statements.DropTableStatement;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * CassandraMetadataEngine Class allow to send metadata queries to the Cassandra Connector.
 */
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

    /**
     * Basic constructor.
     *
     * @param sessions The map of sessions that affect the queries.
     */
    public CassandraMetadataEngine(Map<String, Session> sessions) {
        this.sessions = sessions;
    }

    /**
     * Create Catalog for Cassandra Connector.
     *
     * @param targetCluster   The target cluster.
     * @param catalogMetadata The metadata of the catalog that will be created.
     * @throws ConnectorException
     */
    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());

        String catalogName = catalogMetadata.getName().getQualifiedName();

        Map<Selector, Selector> catalogOptions = catalogMetadata.getOptions();

        String stringOptions = getStringOptions(catalogOptions);

        CreateCatalogStatement catalogStatement =
                new CreateCatalogStatement(catalogName, false, stringOptions);

        CassandraExecutor.execute(catalogStatement.toString(), session);

    }

    @Override public void alterCatalog(ClusterName targetCluster, CatalogName catalogName,
            Map<Selector, Selector> options) throws ConnectorException {

        session = sessions.get(targetCluster.getName());

        String stringOptions = getStringOptions(options);

        AlterCatalogStatement alterCatalogStatement =
                new AlterCatalogStatement(catalogName.getName(), stringOptions);

        CassandraExecutor.execute(alterCatalogStatement.toString(), session);
    }

    /**
     * Create Table for Cassandra Connector.
     *
     * @param targetCluster The target cluster.
     * @param tableMetadata The metadata of the table that will be created.
     * @throws ConnectorException
     */
    @Override
    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata)
            throws ConnectorException {
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
                        primaryKeyType, stringOptions, false);
        CassandraExecutor.execute(tableStatement.toString(), session);

    }

    /**
     * Drop Catalog that was created previously.
     *
     * @param targetCluster The target cluster.
     * @param name          The CatalogName of the Catalog.
     * @throws ConnectorException
     */
    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName name)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());
        DropCatalogStatement catalogStatement = new DropCatalogStatement(name.getName(), false);
        CassandraExecutor.execute(catalogStatement.toString(), session);

    }

    /**
     * Drop table that was created previously.
     *
     * @param targetCluster The target cluster.
     * @param name          The TableName of the Table.
     * @throws ConnectorException
     */
    @Override
    public void dropTable(ClusterName targetCluster, TableName name)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());
        DropTableStatement tableStatement = new DropTableStatement(name.getQualifiedName(), false);
        CassandraExecutor.execute(tableStatement.toString(), session);

    }

    @Override public void alterTable(ClusterName targetCluster, TableName name, AlterOptions alterOptions)
            throws ConnectorException {
        AlterTableStatement tableStatement;
        session = sessions.get(targetCluster.getName());
        switch (alterOptions.getOption()) {
        case ALTER_COLUMN:
        case ADD_COLUMN:
        case DROP_COLUMN:
            ColumnType type = alterOptions.getColumnMetadata().getColumnType();
            tableStatement = new AlterTableStatement(name, alterOptions.getColumnMetadata().getName()
                    , type, alterOptions.getProperties(), alterOptions.getOption());
            CassandraExecutor.execute(tableStatement.toString(), session);
            break;
        case ALTER_OPTIONS:
            tableStatement = new AlterTableStatement(name, null, null, alterOptions.getProperties(),
                    alterOptions.getOption());
            CassandraExecutor.execute(tableStatement.toString(), session);
            break;
        default:
            break;
        }
    }

    /**
     * Create Index for Cassandra Connector.
     *
     * @param targetCluster The target cluster.
     * @param indexMetadata The metadata of the index that will be created.
     * @throws ConnectorException
     */
    @Override
    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());
        CreateIndexStatement indexStatement =
                new CreateIndexStatement(indexMetadata, true, session);
        try {
            CassandraExecutor.execute(indexStatement.toString(), session);
        } catch (ConnectorException e) {
            //remove de column create for the index
            String tableName= Utils.toCaseSensitive(indexMetadata.getName().getTableName().getName());
            String catalog= Utils.toCaseSensitive(indexMetadata.getName().getTableName().getCatalogName().getName());
            String remove = "ALTER TABLE " + catalog + "." + tableName  + " DROP " + indexMetadata
                    .getName().getName();
            CassandraExecutor.execute(remove,session);
            throw e;
        }
    }

    /**
     * Drop Index that was created previously.
     *
     * @param targetCluster The target cluster.
     * @param indexMetadata     The IndexName of the index.
     * @throws ConnectorException
     */
    @Override
    public void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());
        DropIndexStatement indexStatement = new DropIndexStatement(indexMetadata, false);

        //remove de column create for the index and automatically delete the index too
        String tableName= Utils.toCaseSensitive(indexMetadata.getName().getTableName().getName());
        String catalog= Utils.toCaseSensitive(indexMetadata.getName().getTableName().getCatalogName().getName());
        String remove = "ALTER TABLE " + catalog + "." + tableName  + " DROP " + Utils.toCaseSensitive(indexMetadata
                .getName().getName());
        CassandraExecutor.execute(remove,session);
    }

    @Override public List<CatalogMetadata> provideMetadata(ClusterName clusterName) throws ConnectorException {
        session = sessions.get(clusterName.getName());
        return CassandraExecutor.getKeyspaces(session, clusterName.getName());
    }

    @Override public CatalogMetadata provideCatalogMetadata(ClusterName clusterName, CatalogName catalogName)
            throws ConnectorException {
        session = sessions.get(clusterName.getName());
        return CassandraExecutor.getKeyspacesByName(session, catalogName, clusterName.getName());
    }

    @Override public TableMetadata provideTableMetadata(ClusterName clusterName, TableName tableName)
            throws ConnectorException {
        session = sessions.get(clusterName.getName());
        return CassandraExecutor.getTablesByTableName(session, tableName, clusterName.getName());
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
        } else if (createTableOptions.contains(key.toLowerCase())) {
            stringOption.append(key).append(" = ").append(value).append("");
        } else {
            stringOption.append(key).append(" = {")
                    .append(value).append("}");
        }

        return stringOption;
    }

}
