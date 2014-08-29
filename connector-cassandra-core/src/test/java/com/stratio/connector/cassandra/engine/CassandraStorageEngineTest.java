package com.stratio.connector.cassandra.engine;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jjlopez on 29/08/14.
 */
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
        TableName targetTable = new TableName("demo", "demo.users");
        //INSERT INTO demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phase", new Cell("insert phase"));

        try {
            cse.insert(new ClusterName("cluster"), targetTable, row);
            int finalCount = select2InsertTest(_session, query);
            if (finalCount >= initialCount) {
                assert (true);
            } else {
                assert (false);
            }
        } catch (UnsupportedException e) {
            assert (false);
            e.printStackTrace();
        } catch (ExecutionException e) {
            assert (false);
            e.printStackTrace();
        }
    }

    @Test
    public void multipleInsert() {
        String query = "SELECT * FROM demo.users;";
        int initialCount = select2InsertTest(_session, query);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);
        TableName targetTable = new TableName("demo", "demo.users");
        //INSERT INTO demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Collection<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phase", new Cell("insert phase"));

        Row row2 = new Row();
        row2.addCell("name", new Cell("insertName2"));
        row2.addCell("gender", new Cell("female"));
        row2.addCell("email", new Cell("insertName2@doamin.com"));
        row2.addCell("age", new Cell(45));
        row2.addCell("bool", new Cell(true));
        row2.addCell("phase", new Cell("insert phase2"));

        rows.add(row);
        rows.add(row2);

        try {
            cse.insert(new ClusterName("cluster"), targetTable, rows);
            int finalCount = select2InsertTest(_session, query);
            if (finalCount >= initialCount) {
                assert (true);
            } else {
                assert (false);
            }
        } catch (UnsupportedException e) {
            assert (false);
            e.printStackTrace();
        } catch (ExecutionException e) {
            assert (false);
            e.printStackTrace();
        }
    }



}
