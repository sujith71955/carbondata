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

import java.io.Serializable;

import org.carbondata.core.carbon.datastore.CarbonTableSegmentStore;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.carbon.path.CarbonTablePath.DataFileUtil;
import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * class will be used to pass the block detail detail will be passed form driver
 * to all the executor to load the b+ tree
 */
public class TableBlockInfos implements Serializable, Comparable<TableBlockInfos> {

    /**
     * serialization id
     */
    private static final long serialVersionUID = -6502868998599821172L;

    /**
     * full qualified file path of the block
     */
    private String filePath;

    /**
     * block offset in the file
     */
    private long blockOffset;

    /**
     * name of the table to be loaded
     */
    private String tableName;

    /**
     * id of the segment this will be used to sort the blocks
     */
    private int segmentId;

    /**
     * start key of the query
     */
    private byte[] startKey;

    /**
     * end key of the query
     */
    private byte[] endKey;

    /**
     * @return the filePath
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * @param filePath the filePath to set
     */
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    /**
     * @return the blockOffset
     */
    public long getBlockOffset() {
        return blockOffset;
    }

    /**
     * @param blockOffset the blockOffset to set
     */
    public void setBlockOffset(long blockOffset) {
        this.blockOffset = blockOffset;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @param segmentId the segmentId to set
     */
    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (blockOffset ^ (blockOffset >>> 32));
        result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TableBlockInfos)) {
            return false;
        }
        TableBlockInfos other = (TableBlockInfos) obj;
        if (blockOffset != other.blockOffset) {
            return false;
        }
        if (filePath == null) {
            if (other.filePath != null) {
                return false;
            }
        } else if (!filePath.equals(other.filePath)) {
            return false;
        }
        return true;
    }

    /**
     * Below method will used to compare to TableBlockInfos object
     * this will used for sorting
     * Comparison logic is:
     * 1. compare segment id
     * if segment id is same
     * 2. compare file name id
     * if file name id is same
     * 3. compare offsets of the block
     */
    @Override public int compareTo(TableBlockInfos other) {

        int compareResult = 0;
        // get the segment id
        compareResult = segmentId - other.segmentId;
        if (compareResult != 0) {
            return compareResult;
        }
        //@TODO need to compare the file number first than 
        // offset, no sure about the current structure of the 
        //file name  
        if (blockOffset < other.blockOffset) {
            return 1;
        } else if (blockOffset > other.blockOffset) {
            return -1;
        }
        return 0;
    }

    /**
     * @return the startKey
     */
    public byte[] getStartKey() {
        return startKey;
    }

    /**
     * @param startKey the startKey to set
     */
    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    /**
     * @return the endKey
     */
    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * @param endKey the endKey to set
     */
    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

}
