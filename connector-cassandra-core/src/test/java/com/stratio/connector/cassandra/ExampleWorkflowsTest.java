package com.stratio.connector.cassandra;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.engine.CassandraQueryEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class ExampleWorkflowsTest extends BasicCoreCassandraTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        BasicCoreCassandraTest.setUpBeforeClass();
        BasicCoreCassandraTest.loadTestData("example", "exampleKeyspace.cql");
        //metadataManager = new MetadataManager(_session, null);
        //metadataManager.loadMetadata();
    }

    private ExampleWorkflows getExampleWorkflows(){
        ExampleWorkflows exampleWorkflows=new ExampleWorkflows();
        return exampleWorkflows;
    }

    @Test
    public void basicSelect(){
        ClusterName targetCluster = new ClusterName("cluster");

        ExampleWorkflows exampleWorkflows=getExampleWorkflows();
        LogicalWorkflow logicalWorkflow=exampleWorkflows.getBasicSelect();

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, logicalWorkflow);
        } catch (UnsupportedException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        Assert.assertEquals(qr.getResultSet().size(), 4);
    }

    @Test
    public void basicSelectAsterisk(){
        ClusterName targetCluster = new ClusterName("cluster");

        ExampleWorkflows exampleWorkflows=getExampleWorkflows();
        LogicalWorkflow logicalWorkflow=exampleWorkflows.getBasicSelectAsterisk();

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, logicalWorkflow);
        } catch (UnsupportedException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        Assert.assertEquals(qr.getResultSet().size(), 4);
    }

    @Test
    public void selectIndexedField(){
        ClusterName targetCluster = new ClusterName("cluster");

        ExampleWorkflows exampleWorkflows=getExampleWorkflows();
        LogicalWorkflow logicalWorkflow=exampleWorkflows.getSelectIndexedField();

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, logicalWorkflow);
        } catch (UnsupportedException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
        Assert.assertEquals(qr.getResultSet().size(), 1);
    }

    @Test
    public void selectNonIndexedField(){
        ClusterName targetCluster = new ClusterName("cluster");

        ExampleWorkflows exampleWorkflows=getExampleWorkflows();
        LogicalWorkflow logicalWorkflow=exampleWorkflows.getSelectNonIndexedField();

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, logicalWorkflow);
            Assert.assertEquals(qr.getResultSet().size(), 1);
        } catch (UnsupportedException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

    }

    @Test
    public void selectMixedWhere(){
        ClusterName targetCluster = new ClusterName("cluster");

        ExampleWorkflows exampleWorkflows=getExampleWorkflows();
        LogicalWorkflow logicalWorkflow=exampleWorkflows.getSelectMixedWhere();

        Map<String, Session> sessions = new HashMap<>();
        sessions.put("cluster", this._session);
        CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);


        QueryResult qr = null;
        try {
            qr = cqe.execute(targetCluster, logicalWorkflow);
            Assert.assertEquals(qr.getResultSet().size(), 4);
        } catch (UnsupportedException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        } catch (com.stratio.meta.common.exceptions.ExecutionException e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

    }






}
