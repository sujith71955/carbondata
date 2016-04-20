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
package org.carbondata.core.carbon.datastore.block;

import java.util.Arrays;

import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.BtreeBuilder;
import org.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.carbondata.core.carbon.datastore.impl.btree.BlockletBtreeBuilder;
import org.carbondata.core.carbon.metadata.leafnode.DataFileMetadata;

/**
 * Class which is responsible for loading the b+ tree block. This class will
 * persist all the detail of a table block
 */
public class DataBlock {

    /**
     * vo class which will hold the RS information of the block
     */
    private SegmentProperties segmentProperties;

    /**
     * data block
     */
    private DataRefNode dataRefNode;

    /**
     * total number of row present in the block
     */
    private long totalNumberOfRows;

    /**
     * Below method will be used to load the data block
     *
     * @param blockInfo block detail
     */
    public void buildDataBlock(DataFileMetadata dataFileMetadata, String filePath) {
        // create a metadata details
        // this will be useful in query handling
        segmentProperties = new SegmentProperties(dataFileMetadata.getColumnInTable(),
                dataFileMetadata.getSegmentInfo().getColumnCardinality());
        // create a segment builder info
        BTreeBuilderInfo indeBuilderInfo = new BTreeBuilderInfo();
        BtreeBuilder blocksBuilder = new BlockletBtreeBuilder();
        indeBuilderInfo.setDataFileMetadataList(
                Arrays.asList(new DataFileMetadata[] { dataFileMetadata }));
        indeBuilderInfo
                .setEachDimensionBlockSize(segmentProperties.getDimensionColumnsValueSize());
        indeBuilderInfo.setFilePath(filePath);
        // load the metadata
        blocksBuilder.build(indeBuilderInfo);
        dataRefNode = blocksBuilder.get();
    }

    /**
     * @return the totalNumberOfRows
     */
    public long getTotalNumberOfRows() {
        return totalNumberOfRows;
    }

    /**
     * @param totalNumberOfRows the totalNumberOfRows to set
     */
    public void setTotalNumberOfRows(long totalNumberOfRows) {
        this.totalNumberOfRows = totalNumberOfRows;
    }

    /**
     * @return the segmentProperties
     */
    public SegmentProperties getSegmentProperties() {
        return segmentProperties;
    }

    /**
     * @return the dataBlock
     */
    public DataRefNode getDataRefNode() {
        return dataRefNode;
    }
}
