/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;

/**
 * Class which persist the information about the tables present the carbon schemas
 */
public final class CarbonMetadata {

    /**
     * meta data instance
     */
    private static final CarbonMetadata CARBONMETADATAINSTANCE = new CarbonMetadata();

    /**
     * holds the list of tableInfo currently present
     */
    private Map<String, CarbonTable> tableInfoMap;

    private CarbonMetadata() {
        // creating a concurrent map as it will be updated by multiple thread
        tableInfoMap = new ConcurrentHashMap<String, CarbonTable>();
    }

    public static CarbonMetadata getInstance() {
        return CARBONMETADATAINSTANCE;
    }

    /**
     * removed the table information
     *
     * @param tableUniquName
     */
    public void removeTable(String tableUniquName) {
        tableInfoMap.remove(tableUniquName);
    }

    /**
     * clear all the tables
     */
    public void removeAll() {
        tableInfoMap.clear();
    }

    /**
     * method load the table
     *
     * @param tableInfo
     */
    public void loadTableMetadata(TableInfo tableInfo) {
        CarbonTable carbonTable = tableInfoMap.get(tableInfo.getTableUniqueName());
        if (null == carbonTable || carbonTable.getTableLastUpdatedTime() <tableInfo
                .getLastUpdatedTime()) {
            carbonTable = new CarbonTable();
            carbonTable.loadCarbonTable(tableInfo);
            tableInfoMap.put(tableInfo.getTableUniqueName(), carbonTable);
        }
    }

    /**
     * Below method to get the loaded carbon table
     *
     * @param tableUniqueName
     * @return
     */
    public CarbonTable getCarbonTable(String tableUniqueName) {
        return tableInfoMap.get(tableUniqueName);
    }

    /**
     * @return the number of tables present in the schema
     */
    public int getNumberOfTables() {
        return tableInfoMap.size();
    }
}
