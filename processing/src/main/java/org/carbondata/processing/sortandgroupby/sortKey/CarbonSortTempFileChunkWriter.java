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

package org.carbondata.processing.sortandgroupby.sortKey;

import java.io.File;

import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

public class CarbonSortTempFileChunkWriter implements CarbonSortTempFileWriter {
    /**
     * writer
     */
    private CarbonSortTempFileWriter writer;

    /**
     * recordPerLeaf
     */
    private int recordPerLeaf;

    /**
     * CarbonCompressedSortTempFileChunkWriter
     *
     * @param writer
     */
    public CarbonSortTempFileChunkWriter(CarbonSortTempFileWriter writer, int recordPerLeaf) {
        this.writer = writer;
        this.recordPerLeaf = recordPerLeaf;
    }

    /**
     * finish
     */
    public void finish() {
        this.writer.finish();
    }

    /**
     * initialise
     */
    public void initiaize(File file, int entryCount) throws CarbonSortKeyAndGroupByException {
        this.writer.initiaize(file, entryCount);
    }

    /**
     * Below method will be used to write the sort temp file chunk by chunk
     */
    public void writeSortTempFile(Object[][] records) throws CarbonSortKeyAndGroupByException {
        int recordCount = 0;
        Object[][] tempRecords = null;
        while (recordCount < records.length) {
            if (records.length - recordCount < recordPerLeaf) {
                recordPerLeaf = records.length - recordCount;
            }
            tempRecords = new Object[recordPerLeaf][];
            System.arraycopy(records, recordCount, tempRecords, 0, recordPerLeaf);
            recordCount += recordPerLeaf;
            this.writer.writeSortTempFile(tempRecords);
        }

    }
}
