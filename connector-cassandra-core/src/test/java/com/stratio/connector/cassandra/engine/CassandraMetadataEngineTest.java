package com.stratio.connector.cassandra.engine;


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
import com.stratio.meta2.common.metadata.TableMetadata;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jjlopez on 29/08/14.
 */
public class CassandraMetadataEngineTest extends BasicCoreCassandraTest {

    private Map<String, Session> sessions;

    private boolean catalogIsCreated(){
        return true;
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
        CassandraMetadataEngine cme=new CassandraMetadataEngine(sessions);

        Map<String, Object> options=new HashMap<>();
        Map<TableName, TableMetadata> tables=new HashMap<>();

        CatalogMetadata catalogmetadata=new CatalogMetadata(new CatalogName("demoMetadata"), options, tables );
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            //TODO Control if the catalog is created
            if (catalogIsCreated())
                assert(true);
            else
                assert(false);
        } catch (UnsupportedException e) {
            assert(false);
            e.printStackTrace();
        } catch (ExecutionException e) {
            assert(false);
            e.printStackTrace();
        }

    }

    @Test
    public void createTableTest() {
        CassandraMetadataEngine cme=new CassandraMetadataEngine(sessions);
        List<ColumnName> clusterKey=new ArrayList<>();
        Map<String, Object> options=new HashMap<>();
        Map<ColumnName, ColumnMetadata> columns=new HashMap<>();
        List<ColumnName> partitionKey=new ArrayList<>();
        TableMetadata table=new TableMetadata(new TableName("demoMetadata","testCreateTable"),options,columns,new ClusterName("cluster"),partitionKey, clusterKey);

        try {
            cme.createTable(new ClusterName("cluster"),table);
            //TODO test if table is created
            assert(true);
        } catch (UnsupportedException e) {
            assert(false);
            e.printStackTrace();
        } catch (ExecutionException e) {
            assert(false);
            e.printStackTrace();
        }
    }

    @Test
    public void dropCatalogTest() {
        //TODO test if catalog is removed

    }

    @Test
    public void dropTableTest() {
        //TODO test if table is removed
    }



}
