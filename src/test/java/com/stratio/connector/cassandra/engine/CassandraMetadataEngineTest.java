/*
 * Licensed to STRATIO (C) under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership. The STRATIO
 * (C) licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.stratio.connector.cassandra.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class CassandraMetadataEngineTest extends BasicCoreCassandraTest {

    private Map<String, Session> sessions;

    private int assertCatalog() {
        Session session = sessions.get("cluster");
        List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
        int numberOfKS = keyspaces.size();
        return numberOfKS;
    }

    private int assertTable() {
        Session session = sessions.get("cluster");
        Collection<com.datastax.driver.core.TableMetadata> tables =
                _session.getCluster().getMetadata().getKeyspace("demometadata").getTables();
        int numberOfTables = tables.size();
        return numberOfTables;
    }

    private String assertIndex(String columnIndex, String keySpace, String theTable) {
        String indexName = "";
        Session session = sessions.get("cluster");
        com.datastax.driver.core.TableMetadata table =
                session.getCluster().getMetadata().getKeyspace(keySpace).getTable(theTable);
        if (table.getColumn(columnIndex).getIndex() != null) {
            indexName = table.getColumn(columnIndex).getIndex().getName();
        }
        return indexName;
    }

    public void createTable(String tableName) {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        TableName targetTable = new TableName("demometadata", tableName);

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", tableName, "name");
        ColumnName partitionColumn2 = new ColumnName("demometadata", tableName, "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        List<ColumnName> clusterKey = new ArrayList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", tableName), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        try {
            cme.createTable(new ClusterName("cluster"), table);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    public void createCatalog() {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata =
                new CatalogMetadata(new CatalogName("demoMetadata"), options, tables);

        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    public void createIndex() {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometadata", "users1", "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "age"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demometadata", "users1", "Indice2"), columns,
                        IndexType.DEFAULT, null);
        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    @BeforeClass
    public void setUp() {
        BasicCoreCassandraTest.setUpBeforeClass();
        BasicCoreCassandraTest.loadTestData("demo", "demoKeyspace.cql");
        sessions = new HashMap<>();
        sessions.put("cluster", _session);
    }

    @Test
    public void createCatalogTest() {

        int rowsInitial = assertCatalog();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata =
                new CatalogMetadata(new CatalogName("demoMetadata"), options, tables);
        int rowsFinal = rowsInitial;
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            rowsFinal = assertCatalog();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);

    }

    @Test
    public void createCatalogWithOptionsTest() {

        int rowsInitial = assertCatalog();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("REPLICATION"),
                new StringSelector("'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3"));
        options.put(new StringSelector("durable_writes"), new StringSelector("false"));

        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata =
                new CatalogMetadata(new CatalogName("demoMetadata2"), options, tables);
        int rowsFinal = rowsInitial;
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            rowsFinal = assertCatalog();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);

    }

    @Test
    public void createCatalogWithOptionsIncompleteTest() {

        int rowsInitial = assertCatalog();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("REPLICATION"),
                new StringSelector("'class' : 'SimpleStrategy'"));

        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata =
                new CatalogMetadata(new CatalogName("demoMetadata3"), options, tables);
        int rowsFinal = rowsInitial;
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            Assert.fail("Options incomplete");
        } catch (UnsupportedException e) {
            Assert.assertTrue(true);
        } catch (ExecutionException e) {
            Assert.assertTrue(true);
        }catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void createTableTest() {
        int rowsInitial = assertTable();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demometadata", "users");

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users", "name");
        ColumnName partitionColumn2 = new ColumnName("demometadata", "users", "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        List<ColumnName> clusterKey = new ArrayList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTableSinglePKTest() {
        int rowsInitial = assertTable();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demometadata", "users4");

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users4", "name");
        ColumnName clusterColumn1 = new ColumnName("demometadata", "users4", "bool");

        partitionKey.add(partitionColumn1);

        List<ColumnName> clusterKey = new ArrayList<>();
        clusterKey.add(clusterColumn1);

        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTablePKwihtClusterKeyTest() {
        int rowsInitial = assertTable();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demometadata", "users6");

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users6", "name");

        partitionKey.add(partitionColumn1);

        List<ColumnName> clusterKey = new ArrayList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTableTestWithOptions() {
        int rowsInitial = assertTable();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        StringSelector option1 = new StringSelector("insertion_time DESC");
        StringSelector option2 = new StringSelector("Yes");
        StringSelector option3 = new StringSelector("'Important biological records'");
        //options.put(new StringSelector("CLUSTERING ORDER BY"), option1);
        //options.put(new StringSelector("COMPACT STORAGE"), option2);
        //options.put(new StringSelector("comment"), option3);
        options.put(new StringSelector("compression"), new StringSelector(
                "'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 64"));

        TableName targetTable = new TableName("demometadata", "users3");

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users3", "name");
        ColumnName partitionColumn2 = new ColumnName("demometadata", "users3", "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        List<ColumnName> clusterKey = new ArrayList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void dropTest() {
        createCatalog();
        createTable("users1");
        createIndex();

        //drop Index
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users1", "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "age"),
                        parameters, ColumnType.TEXT));
        IndexMetadata index =
                new IndexMetadata(new IndexName("demometadata", "users1", "Indice2"), columns,
                        IndexType.DEFAULT, null);
        try {
            cme.dropIndex(new ClusterName("cluster"), index);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(assertIndex("age", "demometadata", "users1"), "Indice2");

        //drop table
        int rowsInitial = assertTable();
        cme = new CassandraMetadataEngine(sessions);
        int rowsFinal = rowsInitial;
        try {
            cme.dropTable(new ClusterName("cluster"), new TableName("demometadata", "users1"));
            rowsFinal = assertTable();
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);

        //drop catalog
        rowsInitial = assertCatalog();
        rowsFinal = rowsInitial;
        try {
            cme.dropCatalog(new ClusterName("cluster"), new CatalogName("demometadata"));
            rowsFinal = assertCatalog();
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createSimpleIndexTest() {
        createCatalog();
        createTable("users1");

        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users1", "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "email"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demometadata", "users1", "Indice"), columns,
                        IndexType.DEFAULT, null);

        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(assertIndex("email", "demometadata", "users1"), "indice");
    }

    @Test
    public void createSimpleIndexWithoutNameTest() {
        createCatalog();
        createTable("users5");

        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users5", "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users5"), "email"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName(new TableName("demometadata", "users5"), "email"), columns,
                        IndexType.DEFAULT, null);

        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(assertIndex("email", "demometadata", "users5"), "email");
    }

    @Test
    public void createLuceneIndexTest() {
        createCatalog();
        createTable("users1");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users1", "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "phrase"),
                        parameters, ColumnType.TEXT));

        Map<Selector, Selector> options = new LinkedHashMap<>();

        List<ColumnMetadata> columnsIndex = new ArrayList<>();
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "name"),
                        parameters, ColumnType.TEXT));
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "age"),
                        parameters, ColumnType.INT));
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "email"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demometadata", "users1", "IndiceLucene"), columns,
                        IndexType.FULL_TEXT, options);
        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(assertIndex("phrase", "demometadata", "users1"), "stratio_lucene_indicelucene");
    }

    @AfterClass
    public void restore() {
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata");
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata2");
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata3");
    }

}
