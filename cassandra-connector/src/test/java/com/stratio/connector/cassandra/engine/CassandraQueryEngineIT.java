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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandra;
import com.stratio.connector.cassandra.ResultHandler;
import com.stratio.connector.cassandra.statements.SelectStatement;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.OrderBy;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.AsteriskSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class CassandraQueryEngineIT extends BasicCoreCassandra {

    @BeforeClass
    public static void setUpBeforeClass() {
        BasicCoreCassandra.setUpBeforeClass();
        BasicCoreCassandra.loadTestData("cassandra_connector_demo", "demoKeyspace.cql");
    }

    @Test
    public void basicSelectTest() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);

        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_LIMIT);
        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);

        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), "name");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();
        typeMap.put("cassandra_connector_demo.users.name", new ColumnType(DataType.VARCHAR));
        typeMapFromColumnName.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), new ColumnType(DataType.VARCHAR));
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        filter2.setNextStep(select);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

        String value = "";
        for (Row row : qr.getResultSet()) {
            Cell cell = row.getCell("name");
            value = cell.getValue().toString();
        }
        assertEquals(value, "name_5", "The value not match with the expected value");

        SelectStatement ss=new SelectStatement(workflow,100,sessions, connectorOptionsPerCluster);
        assertEquals(ss.parseQuery(),
                "SELECT \"name\" FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' LIMIT 100",
                "The select statement not match with the expected value");

    }

    @Test
    public void selectOrderByTest() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);

        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_ORDER_BY);
        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);


        List<OrderByClause> listOrderBy=new ArrayList<>();
        Selector columnSelector=new ColumnSelector(new ColumnName("cassandra_connector_demo","users","email"));
        OrderByClause orderByClause=new OrderByClause(OrderDirection.ASC,columnSelector);
        listOrderBy.add(orderByClause);
        OrderBy orderBy=new OrderBy(operationsSet2,listOrderBy);

        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), "name");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();
        typeMap.put("cassandra_connector_demo.users.name", new ColumnType(DataType.VARCHAR));
        typeMapFromColumnName.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), new ColumnType(DataType.VARCHAR));
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        orderBy.setNextStep(select);
        filter2.setNextStep(orderBy);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

        SelectStatement ss=new SelectStatement(workflow,100,sessions,connectorOptionsPerCluster);
        assertEquals(ss.parseQuery(),
                "SELECT \"name\" FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' ORDER BY \"email\" ASC LIMIT 100",
                "The select statement not match with the expected value");

    }

    @Test
    public void basicSelectWithOwnLimitTest() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);

        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_LIMIT);
        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);

        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        Limit limit = new Limit(operationsSet2, 50);

        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), "name");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();
        typeMap.put("cassandra_connector_demo.users.name", new ColumnType(DataType.VARCHAR));
        typeMapFromColumnName.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), new ColumnType(DataType.VARCHAR));
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        limit.setNextStep(select);
        filter2.setNextStep(limit);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }


        String value = "";
        for (Row row : qr.getResultSet()) {
            Cell cell = row.getCell("name");
            value = cell.getValue().toString();
        }
        assertEquals(value, "name_5", "The value not match with the expected value");

        SelectStatement ss=new SelectStatement(workflow,100,sessions,connectorOptionsPerCluster);
        assertEquals(ss.parseQuery(),
                "SELECT \"name\" FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' LIMIT 50",
                "The select statement query obtained not match with the expected query");

    }

    @Test
    public void SelectTestWithAlias() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        ColumnName columnName2 = new ColumnName(tableName, "gender");
        columnList.add(columnName);
        columnList.add(columnName2);

        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);

        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.FILTER_INDEXED_EQ);
        Relation relation2 = new Relation(selector2, Operator.EQ, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);

        Relation relation = new Relation(selector, Operator.EQ, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), "nameAlias");
        aliasColumns.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender")), "gender");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();
        typeMap.put("cassandra_connector_demo.users.name", new ColumnType(DataType.VARCHAR));
        typeMap.put("cassandra_connector_demo.users.gender", new ColumnType(DataType.VARCHAR));
        typeMapFromColumnName.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), new ColumnType(DataType.VARCHAR));
        typeMapFromColumnName.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender")), new ColumnType(DataType.VARCHAR));
        Set<Operations> operationsSet3=new HashSet<>();
        operationsSet3.add(Operations.SELECT_LIMIT);
        Select aliasSelect = new Select(operationsSet3, aliasColumns, typeMap, typeMapFromColumnName);

        //Compound workflow
        filter2.setNextStep(aliasSelect);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }


        String value = "";
        try {
            for (Row row : qr.getResultSet()) {
                Cell cell = row.getCell("nameAlias");
                value = cell.getValue().toString();
            }
            assertEquals(value, "name_5", "The value not match with the expected value");

            SelectStatement ss=new SelectStatement(workflow,100,sessions,connectorOptionsPerCluster);
            assertEquals(ss.parseQuery(),
                    "SELECT \"name\",\"gender\" FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' LIMIT 100",
                    "The select query obtained not match with the expected query");
        } catch (Exception ex) {
            Assert.fail("No alias found");
        }

    }
    @Test
    public void countFunctionTest(){
        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();


        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);



        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_LIMIT);
        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);

        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        //Function count
        List<Selector> functionColumns=new ArrayList<Selector>();
        Selector asteriskSelector=new AsteriskSelector();
        functionColumns.add(asteriskSelector);

        Selector functionSelector=new FunctionSelector("Count", functionColumns);
        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(functionSelector,"count");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();

        typeMapFromColumnName.put(functionSelector, new ColumnType(DataType.VARCHAR));


        operationsSet2.add(Operations.SELECT_FUNCTIONS);
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        filter2.setNextStep(select);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }


        SelectStatement ss=new SelectStatement(workflow,100,sessions,connectorOptionsPerCluster);
        assertEquals(ss.parseQuery(),
                "SELECT Count(*) FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' LIMIT 100",
                "The select statement not match with the expected value");

    }


    @Test
    public void nowFunctionTest(){
        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();


        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);

        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_LIMIT);
        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);

        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        //Function count
        List<Selector> functionColumns=new ArrayList<Selector>();
        //Selector asteriskSelector=new AsteriskSelector();
        //functionColumns.add(asteriskSelector);

        Selector functionSelector=new FunctionSelector("now", functionColumns);
        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(functionSelector,"now");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();

        typeMapFromColumnName.put(functionSelector, new ColumnType(DataType.VARCHAR));
        operationsSet2.add(Operations.SELECT_FUNCTIONS);
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        filter2.setNextStep(select);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

        SelectStatement ss=new SelectStatement(workflow,100,sessions,connectorOptionsPerCluster);
        assertEquals(ss.parseQuery(),
                "SELECT now() FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' LIMIT 100",
                "The select statement not match with the expected value");

    }


    @Test
    public void functionWithParametersTest(){
        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();


        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);

        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_LIMIT);
        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);

        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        //Function count
        List<Selector> functionColumns=new ArrayList<Selector>();
        Selector columnSelector=new ColumnSelector(new ColumnName("cassandra_connector_demo","users","phrase"));
        functionColumns.add(columnSelector);

        Selector functionSelector=new FunctionSelector("ttl", functionColumns);
        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(functionSelector,"ttl");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();

        typeMapFromColumnName.put(functionSelector, new ColumnType(DataType.VARCHAR));

        operationsSet2.add(Operations.SELECT_FUNCTIONS);
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        filter2.setNextStep(select);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }



        SelectStatement ss=new SelectStatement(workflow,100,sessions,connectorOptionsPerCluster);
        assertEquals(ss.parseQuery(),
                "SELECT ttl(\"phrase\") FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' LIMIT 100",
                "The select statement not match with the expected value");

    }


    @Test
    public void LuceneSelectTest() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);

        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "phrase"));
        Selector rightTerm = new StringSelector("*");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_LIMIT);
        Relation relation = new Relation(selector, Operator.MATCH, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), "name");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();
        typeMap.put("cassandra_connector_demo.users.name", new ColumnType(DataType.VARCHAR));
        typeMapFromColumnName.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")),
                new ColumnType(DataType.VARCHAR));
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        filter.setNextStep(select);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);

        QueryResult qr = null;
        try {
            qr = cqe.execute("queryid",workflow);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

        String value = "";
        for (Row row : qr.getResultSet()) {
            Cell cell = row.getCell("name");
            value = cell.getValue().toString();
        }

        assertNotEquals(value, null, "The value cannot be null");
    }


    @Test
    public void basicSelectAsyncTest() {

        ClusterName targetCluster = new ClusterName("cluster");

        List<LogicalStep> logicalSteps = new ArrayList<>();

        TableName tableName = new TableName("cassandra_connector_demo", "users");

        List<ColumnName> columnList = new ArrayList<>();
        ColumnName columnName = new ColumnName(tableName, "name");
        columnList.add(columnName);

        //Generation of Data
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.PROJECT);
        Project project = new Project(operationsSet, tableName, targetCluster, columnList);


        Selector selector = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name"));
        Selector rightTerm = new StringSelector("name_5");

        Selector selector2 = new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "gender"));
        Selector rightTerm2 = new StringSelector("female");

        Set<Operations> operationsSet2=new HashSet<>();
        operationsSet2.add(Operations.SELECT_LIMIT);
        Relation relation2 = new Relation(selector2, Operator.ASSIGN, rightTerm2);
        Filter filter2 = new Filter(operationsSet2, relation2);

        Relation relation = new Relation(selector, Operator.ASSIGN, rightTerm);
        Filter filter = new Filter(operationsSet2, relation);

        Map<Selector, String> aliasColumns = new LinkedHashMap<>();
        aliasColumns.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), "name");

        Map<String, ColumnType> typeMap = new HashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new HashMap<>();
        typeMap.put("cassandra_connector_demo.users.name", new ColumnType(DataType.VARCHAR));
        typeMapFromColumnName.put(new ColumnSelector(new ColumnName("cassandra_connector_demo", "users", "name")), new ColumnType(DataType.VARCHAR));
        Select select = new Select(operationsSet2, aliasColumns, typeMap, typeMapFromColumnName);


        //Compound workflow
        filter2.setNextStep(select);
        filter.setNextStep(filter2);
        project.setNextStep(filter);
        logicalSteps.add(project);
        LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);


        try {
            String queryId="1234";
            IResultHandler resultHandler=new ResultHandler();
            cqe.asyncExecute(queryId, workflow, resultHandler);

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }



        SelectStatement ss=new SelectStatement(workflow,100,sessions,connectorOptionsPerCluster);
        assertEquals(ss.parseQuery(),
                "SELECT \"name\" FROM \"cassandra_connector_demo\".\"users\" WHERE \"name\" = 'name_5' AND \"gender\" = 'female' LIMIT 100",
                "The select statement not match with the expected value");

    }
    

    @Test
    public void basicSelectAsyncStopTest() {

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        List<Pair<String,String>> propertiesList = new ArrayList<>();
        Pair<String,String> property= new ImmutablePair<>("DefaultLimit", "100");
        propertiesList.add(property);
        Map<String,List<Pair<String,String>>> connectorOptionsPerCluster=new HashMap<>();
        connectorOptionsPerCluster.put("cluster", propertiesList);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions, connectorOptionsPerCluster);


        try {
            cqe.stop("QueryID");
            Assert.fail();
        } catch (UnsupportedException e) {
            Assert.assertTrue(true);
        } catch (ExecutionException e) {
            Assert.fail(e.getMessage());
        }catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }


    @AfterClass
    public void restore() {
        BasicCoreCassandra.dropKeyspaceIfExists("cassandra_connector_demo");
    }

}
