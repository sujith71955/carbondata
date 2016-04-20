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

import java.util.List;

import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.BtreeBuilder;
import org.carbondata.core.carbon.datastore.IndexesBuilderInfo;
import org.carbondata.core.carbon.datastore.impl.btree.BlockBtreeBuilder;
import org.carbondata.core.carbon.metadata.leafnode.DataFileMetadata;

/**
 * Class which is responsible for loading the b+ tree block. This class will
 * persist all the detail of a table segment
 */
public class TableSegment {

    /**
     * vo class which will hold the RS information of the block
     */
    private SegmentProperties segmentProperties;

    /**
     * to store the segment meta data in some data structure
     */
    private DataRefNode dataRefBlock;

    /**
     * Below method is store the blocks in some data structure
     *
     * @param blockInfo block detail
     */
    public void loadSegmentBlock(List<DataFileMetadata> datFileMetadataList, String filePath) {
        // create a metadata details
        // this will be useful in query handling
        // all the data file metadata will have common segment properties we
        // can use first one to get create the segment properties
        segmentProperties = new SegmentProperties(datFileMetadataList.get(0).getColumnInTable(),
        		datFileMetadataList.get(0).getSegmentInfo().getColumnCardinality());
        // create a segment builder info
        IndexesBuilderInfo segmentBuilderInfos = new IndexesBuilderInfo();
        segmentBuilderInfos.setDataFileMetadataList(datFileMetadataList);
        BtreeBuilder blocksBuilder = new BlockBtreeBuilder();
        // load the metadata
        blocksBuilder.build(segmentBuilderInfos);
        dataRefBlock = blocksBuilder.get();
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
    public DataRefNode getDataBlock() {
        return dataRefBlock;
    }

}
