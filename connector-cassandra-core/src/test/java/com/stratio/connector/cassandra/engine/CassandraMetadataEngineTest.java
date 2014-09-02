package com.stratio.connector.cassandra.engine;


import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Created by jjlopez on 29/08/14.
 */
public class CassandraMetadataEngineTest extends BasicCoreCassandraTest {

    private Map<String, Session> sessions;

    private int assertCatalog(){
        Session session=sessions.get("cluster");
        List<KeyspaceMetadata> keyspaces=session.getCluster().getMetadata().getKeyspaces();
        int numberOfKS = keyspaces.size();
        return numberOfKS;
    }

    private int assertTable(){
        Session session=sessions.get("cluster");
        Collection<com.datastax.driver.core.TableMetadata> tables=_session.getCluster().getMetadata().getKeyspace("demometadata").getTables();
        int numberOfTables = tables.size();
        return numberOfTables;
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

        int rowsInitial= assertCatalog();
        CassandraMetadataEngine cme=new CassandraMetadataEngine(sessions);

        Map<String, Object> options=new HashMap<>();
        Map<TableName, TableMetadata> tables=new HashMap<>();

        CatalogMetadata catalogmetadata=new CatalogMetadata(new CatalogName("demoMetadata"), options, tables );
        int rowsFinal=rowsInitial;
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            rowsFinal= assertCatalog();

        } catch (UnsupportedException e) {

            e.printStackTrace();
        } catch (ExecutionException e) {

            e.printStackTrace();
        }
        Assert.assertNotEquals(rowsInitial,rowsFinal);

    }

    @Test
    public void createTableTest() {
        int rowsInitial= assertTable();
        CassandraMetadataEngine cme=new CassandraMetadataEngine(sessions);

        Map<String, Object> options=new HashMap<>();


        TableName targetTable=new TableName("demometadata", "users");

        Map< ColumnName, ColumnMetadata > columns=new HashMap<>();
        ClusterName clusterRef=new ClusterName("cluster");
        List<ColumnName> partitionKey=new ArrayList<>();
        ColumnName partitionColumn1=new ColumnName("demometadata","users","name");
        ColumnName partitionColumn2=new ColumnName("demometadata","users","gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        List<ColumnName> clusterKey=new ArrayList<>();
        Object[] parameters=null;
        columns.put(new ColumnName(new TableName("demometadata","users"),"name"),new ColumnMetadata(new ColumnName(new TableName("demometadata","users"),"name"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata","users"),"gender"),new ColumnMetadata(new ColumnName(new TableName("demometadata","users"),"gender"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata","users"),"age"),new ColumnMetadata(new ColumnName(new TableName("demometadata","users"),"age"),parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata","users"),"bool"),new ColumnMetadata(new ColumnName(new TableName("demometadata","users"),"bool"),parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata","users"),"phrase"),new ColumnMetadata(new ColumnName(new TableName("demometadata","users"),"phrase"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata","users"),"email"),new ColumnMetadata(new ColumnName(new TableName("demometadata","users"),"email"),parameters, ColumnType.TEXT));

        TableMetadata table=new TableMetadata(targetTable,options,columns,clusterRef,partitionKey,clusterKey);

        int rowsFinal=rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"),table);
            rowsFinal= assertTable();

        } catch (UnsupportedException e) {

            e.printStackTrace();
        } catch (ExecutionException e) {

            e.printStackTrace();
        }
        Assert.assertNotEquals(rowsInitial,rowsFinal);
    }

    @Test
    public void dropTest() {

        //drop table
        int rowsInitial=assertTable();
        CassandraMetadataEngine cme=new CassandraMetadataEngine(sessions);
        int rowsFinal=rowsInitial;
        try {
            cme.dropTable(new ClusterName("cluster"),new TableName("demometadata","users"));
            rowsFinal=assertTable();
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        Assert.assertNotEquals(rowsInitial,rowsFinal);

        //drop catalog
        rowsInitial=assertCatalog();
        rowsFinal=rowsInitial;
        try {
            cme.dropCatalog(new ClusterName("cluster"), new CatalogName("demoMetadata"));
            rowsFinal=assertCatalog();
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        Assert.assertNotEquals(rowsInitial,rowsFinal);
    }

}
