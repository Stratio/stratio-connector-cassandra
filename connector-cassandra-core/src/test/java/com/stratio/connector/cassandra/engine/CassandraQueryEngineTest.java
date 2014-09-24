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



import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.connector.cassandra.data.CassandraResultSet;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.*;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.*;

public class CassandraQueryEngineTest extends BasicCoreCassandraTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        BasicCoreCassandraTest.setUpBeforeClass();
        BasicCoreCassandraTest.loadTestData("demo", "demoKeyspace.cql");
        //metadataManager = new MetadataManager(_session, null);
        //metadataManager.loadMetadata();
    }





    @Test
    public void basicSelectTest() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Project project = new Project(Operations.PROJECT, tableName);
        ColumnName selectColumn=new ColumnName("demo","users","name");
        project.addColumn(selectColumn);

        Selector selector = new ColumnSelector(new ColumnName("demo", "users", "name"));
        Selector rightTerm = new StringSelector("'name_5'");

        Selector selector2 = new ColumnSelector(new ColumnName("demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("'female'");

        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(Operations.SELECT_LIMIT, relation2);

        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(Operations.SELECT_LIMIT, relation);

        //Compound workflow
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, workflow);
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
        }

        String value = "";
        for (Row row : qr.getResultSet()) {
            Cell cell = row.getCell("name");
            value = cell.getValue().toString();
        }
        assertEquals(value, "name_5");

        assertEquals(cqe.parseQuery(),
            "SELECT name FROM demo.users WHERE name = 'name_5' AND gender = 'female'");

    }


    @Test
    public void SelectTestWithAlias() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Project project = new Project(Operations.PROJECT, tableName);
        ColumnName selectColumn=new ColumnName("demo","users","name");
        project.addColumn(selectColumn);

        Selector selector = new ColumnSelector(new ColumnName("demo", "users", "name"));
        Selector rightTerm = new StringSelector("'name_5'");

        Selector selector2 = new ColumnSelector(new ColumnName("demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("'female'");

        Relation relation2 = new Relation(selector2, Operator.EQ, rightTerm2);
        Filter filter2 = new Filter(Operations.FILTER_INDEXED_EQ , relation2);

        Relation relation = new Relation(selector, Operator.EQ, rightTerm);
        Filter filter = new Filter(Operations.FILTER_NON_INDEXED_EQ, relation);

        Map<String, String> aliasColumns=new HashMap<>();
        aliasColumns.put("demo.users.name", "nameAlias");

        Map<String, ColumnType> typeMap=new HashMap<>();
        typeMap.put("demo.users.name", ColumnType.VARCHAR);
        Select aliasSelect=new Select(Operations.SELECT_LIMIT, aliasColumns, typeMap);

        //Compound workflow
        filter2.setNextStep(aliasSelect);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, workflow);
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
        }

        String value = "";
        try {
            for (Row row : qr.getResultSet()) {
                Cell cell = row.getCell("nameAlias");
                value = cell.getValue().toString();
            }
            assertEquals(value, "name_5");

            assertEquals(cqe.parseQuery(),
                "SELECT name FROM demo.users WHERE name = 'name_5' AND gender = 'female'");
        }catch (Exception ex){
            Assert.fail("No alias found");
        }

    }



    @Test
    public void LuceneSelectTest() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Project project = new Project(Operations.PROJECT, tableName);
        ColumnName selectColumn=new ColumnName("demo","users","name");
        project.addColumn(selectColumn);

        Selector selector = new ColumnSelector(new ColumnName("demo", "users", "phrase"));
        Selector rightTerm = new StringSelector("*");

        Relation relation = new Relation(selector, Operator.MATCH, rightTerm);
        Filter filter = new Filter(Operations.SELECT_LIMIT, relation);

        //Compound workflow
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, workflow);
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
        }

        String value = "";
        for (Row row : qr.getResultSet()) {
            Cell cell = row.getCell("name");
            value = cell.getValue().toString();
        }
        if (value!=null && !value.equals(""))
            assertEquals(true, true);

        //assertEquals(cqe.parseQuery(), "SELECT name FROM demo.users WHERE name = 'name_5' AND gender = 'female'");

    }



    @Test
    public void processLuceneQueryType(){

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);

        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);
        String [][] queries = {
            //Input    Type       parsed
            {"?",     "wildcard", "?"},
            {"*",     "wildcard", "*"},
            {"\\?",   "match",    "?"},
            {"\\*",   "match",    "*"},
            {"\\?sf", "match",    "?sf"},
            {"af\\?", "match",    "af?"},
            {"s\\?f", "match",    "s?f"},
            {"sdf",   "match",    "sdf"},
            {"*asd*", "wildcard", "*asd*"},
            {"?as?",  "wildcard", "?as?"},
            {"?as*",  "wildcard", "?as*"},
            {"[asd",  "regex",    "[asd"},
            {"fa]",   "regex",    "fa]"},
            {"]*sf",  "regex",    "]*sf"},
            {"~as",   "match",    "~as"},
            {"as~2",  "fuzzy",    "as~2"}};

        for(String [] query : queries) {
            String [] result = cqe.processLuceneQueryType(query[0]);
            assertEquals(result[0], query[1], "Query type does not match");
            assertEquals(result[1], query[2], "Parsed does not match");
        }

    }

    @AfterClass
    public void restore(){
        BasicCoreCassandraTest.dropKeyspaceIfExists("demo");
    }


}
