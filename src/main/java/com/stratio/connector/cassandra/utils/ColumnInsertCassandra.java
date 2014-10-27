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
package com.stratio.connector.cassandra.utils;

import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * ColumnInsertCassandra Class that store the type and value of a Cassandra column.
 */
public class ColumnInsertCassandra {

    private ColumnType type;
    private String value;
    private String columnName;

    /**
     * Basic Constructor.
     * @param type Type of the Column.
     * @param value Value of the Column.
     * @param columnName String with the column name.
     */
    public ColumnInsertCassandra(ColumnType type, String value, String columnName) {
        this.type = type;
        this.value = value;
        this.columnName = columnName;
    }

    /**
     * Get the type
     * @return ColumnType
     */
    public ColumnType getType() {
        return type;
    }

    /**
     * Set the type
     * @param type
     */
    public void setType(ColumnType type) {
        this.type = type;
    }

    /**
     * Get the value
     * @return String with the value of a Column.
     */
    public String getValue() {
        return value;
    }

    /**
     * Set the value of a column.
     * @param value
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Get the String with the column name.
     * @return String.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Set the column name.
     * @param columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
