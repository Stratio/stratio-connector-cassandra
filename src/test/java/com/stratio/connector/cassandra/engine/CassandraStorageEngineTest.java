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
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

public class CassandraStorageEngineTest extends BasicCoreCassandraTest {

    private Map<String, Session> sessions;

    public int select2InsertTest(Session session, String query) {

        ResultSet res = session.execute(query);
        int numberOfRows = res.all().size();
        return numberOfRows;
    }

    @BeforeClass
    public void setUp() {
        BasicCoreCassandraTest.setUpBeforeClass();
        BasicCoreCassandraTest.loadTestData("demo", "demoKeyspace.cql");
        sessions = new HashMap<>();
        sessions.put("cluster", _session);
    }

    @Test
    public void basicInsert() {
        String query = "SELECT * FROM demo.users;";
        int initialCount = select2InsertTest(_session, query);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        TableName targetTable = new TableName("demo", "users");
        Map<Selector, Selector> options = new HashMap<>();
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        List<ColumnName> clusterKey = new ArrayList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "name"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "gender"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "age"), parameters,
                        ColumnType.INT));
        columns.put(new ColumnName(new TableName("demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "bool"), parameters,
                        ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "phrase"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "email"), parameters,
                        ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        //INSERT INTO demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phrase", new Cell("insert phase"));

        int finalCount = initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, row);
            finalCount = select2InsertTest(_session, query);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(initialCount, finalCount);
    }

    @Test
    public void InsertInExistingTableUnknownColumn() {
        String query = "SELECT * FROM demo.users;";
        int initialCount = select2InsertTest(_session, query);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        TableName targetTable = new TableName("demo", "users");
        Map<Selector, Selector> options = new HashMap<>();
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        List<ColumnName> clusterKey = new ArrayList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "name"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "gender"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "age"), parameters,
                        ColumnType.INT));
        columns.put(new ColumnName(new TableName("demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "bool"), parameters,
                        ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "phrase"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "email"), parameters,
                        ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        //INSERT INTO demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        //WRONG COLUMN
        row.addCell("prase", new Cell("insert phase"));

        int finalCount = initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, row);
            Assert.fail("Trying to add in a not existing column");
        }catch (ConnectorException e) {
            finalCount = select2InsertTest(_session, query);
        }
        Assert.assertEquals(initialCount, finalCount);
    }

    @Test
    public void multipleInsert() {
        String query = "SELECT * FROM demo.users;";
        int initialCount = select2InsertTest(_session, query);

        TableName targetTable = new TableName("demo", "users");
        Map<Selector, Selector> options = new HashMap<>();
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        List<ColumnName> partitionKey = new ArrayList<>();
        List<ColumnName> clusterKey = new ArrayList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "name"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "gender"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "age"), parameters,
                        ColumnType.INT));
        columns.put(new ColumnName(new TableName("demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "bool"), parameters,
                        ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "phrase"), parameters,
                        ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demo", "users"), "email"), parameters,
                        ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        //INSERT INTO demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Collection<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.addCell("name", new Cell("insertName3"));
        row.addCell("gender", new Cell("male3"));
        row.addCell("email", new Cell("insertName3@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phrase", new Cell("insert phase3"));

        Row row2 = new Row();
        row2.addCell("name", new Cell("insertName2"));
        row2.addCell("gender", new Cell("female"));
        row2.addCell("email", new Cell("insertName2@doamin.com"));
        row2.addCell("age", new Cell(45));
        row2.addCell("bool", new Cell(true));
        row2.addCell("phrase", new Cell("insert phase2"));

        rows.add(row);
        rows.add(row2);

        int finalCount = initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, rows);
            finalCount = select2InsertTest(_session, query);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(initialCount, finalCount);
    }

    @AfterClass
    public void restore() {
        BasicCoreCassandraTest.dropKeyspaceIfExists("demo");
    }

}
