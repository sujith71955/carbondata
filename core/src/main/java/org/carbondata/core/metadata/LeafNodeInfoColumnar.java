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

package org.carbondata.core.metadata;

import org.carbondata.core.keygenerator.mdkey.NumberCompressor;

public class LeafNodeInfoColumnar {
    /**
     * fileName.
     */
    private String fileName;

    /**
     * measureOffset.
     */
    private long[] measureOffset;

    /**
     * measureLength.
     */
    private int[] measureLength;

    /**
     * numberOfKeys.
     */
    private int numberOfKeys;

    /**
     * startKey.
     */
    private byte[] startKey;

    /**
     * endKey.
     */
    private byte[] endKey;

    /**
     * keyOffSets
     */
    private long[] keyOffSets;

    /**
     * keyLengths
     */
    private int[] keyLengths;

    /**
     * isSortedKeyColumn
     */
    private boolean[] isSortedKeyColumn;

    /**
     * keyBlockIndexOffSets
     */
    private long[] keyBlockIndexOffSets;

    /**
     * keyBlockIndexLength
     */
    private int[] keyBlockIndexLength;

    /**
     * dataIndexMap
     */
    private int[] dataIndexMapLength;

    /**
     * dataIndexMap
     */
    private long[] dataIndexMapOffsets;

    private boolean[] aggKeyBlock;
    /**
     * leafNodeMetaSize
     */
    private int leafNodeMetaSize;

    private NumberCompressor[] keyBlockUnCompressor;

    /**
     * column min max array
     */
    private byte[][] columnMinMaxData;

    /**
     * getFileName().
     *
     * @return String.
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * setFileName.
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * getMeasureLength
     *
     * @return int[].
     */
    public int[] getMeasureLength() {
        return measureLength;
    }

    /**
     * setMeasureLength.
     *
     * @param measureLength
     */
    public void setMeasureLength(int[] measureLength) {
        this.measureLength = measureLength;
    }

    /**
     * getMeasureOffset.
     *
     * @return long[].
     */
    public long[] getMeasureOffset() {
        return measureOffset;
    }

    /**
     * setMeasureOffset.
     *
     * @param measureOffset
     */
    public void setMeasureOffset(long[] measureOffset) {
        this.measureOffset = measureOffset;
    }

    /**
     * getStartKey().
     *
     * @return byte[].
     */
    public byte[] getStartKey() {
        return startKey;
    }

    /**
     * setStartKey.
     *
     * @param startKey
     */
    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    /**
     * getEndKey().
     *
     * @return byte[].
     */
    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * setEndKey.
     *
     * @param endKey
     */
    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    /**
     * @return the keyOffSets
     */
    public long[] getKeyOffSets() {
        return keyOffSets;
    }

    /**
     * @param keyOffSets the keyOffSets to set
     */
    public void setKeyOffSets(long[] keyOffSets) {
        this.keyOffSets = keyOffSets;
    }

    /**
     * @return the keyLengths
     */
    public int[] getKeyLengths() {
        return keyLengths;
    }

    //TODO SIMIAN

    /**
     * @param keyLengths the keyLengths to set
     */
    public void setKeyLengths(int[] keyLengths) {
        this.keyLengths = keyLengths;
    }

    /**
     * getNumberOfKeys()
     *
     * @return int.
     */
    public int getNumberOfKeys() {
        return numberOfKeys;
    }

    /**
     * setNumberOfKeys.
     *
     * @param numberOfKeys
     */
    public void setNumberOfKeys(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    /**
     * @return the isSortedKeyColumn
     */
    public boolean[] getIsSortedKeyColumn() {
        return isSortedKeyColumn;
    }

    /**
     * @param isSortedKeyColumn the isSortedKeyColumn to set
     */
    public void setIsSortedKeyColumn(boolean[] isSortedKeyColumn) {
        this.isSortedKeyColumn = isSortedKeyColumn;
    }

    /**
     * @return the keyBlockIndexOffSets
     */
    public long[] getKeyBlockIndexOffSets() {
        return keyBlockIndexOffSets;
    }

    /**
     * @param keyBlockIndexOffSets the keyBlockIndexOffSets to set
     */
    public void setKeyBlockIndexOffSets(long[] keyBlockIndexOffSets) {
        this.keyBlockIndexOffSets = keyBlockIndexOffSets;
    }

    /**
     * @return the keyBlockIndexLength
     */
    public int[] getKeyBlockIndexLength() {
        return keyBlockIndexLength;
    }

    /**
     * @param keyBlockIndexLength the keyBlockIndexLength to set
     */
    public void setKeyBlockIndexLength(int[] keyBlockIndexLength) {
        this.keyBlockIndexLength = keyBlockIndexLength;
    }

    /**
     * @return the leafNodeMetaSize
     */
    public int getLeafNodeMetaSize() {
        return leafNodeMetaSize;
    }

    /**
     * @param leafNodeMetaSize the leafNodeMetaSize to set
     */
    public void setLeafNodeMetaSize(int leafNodeMetaSize) {
        this.leafNodeMetaSize = leafNodeMetaSize;
    }

    /**
     * @return the dataIndexMapLenght
     */
    public int[] getDataIndexMapLength() {
        return dataIndexMapLength;
    }

    public void setDataIndexMapLength(int[] dataIndexMapLength) {
        this.dataIndexMapLength = dataIndexMapLength;
    }

    /**
     * @return the dataIndexMapOffsets
     */
    public long[] getDataIndexMapOffsets() {
        return dataIndexMapOffsets;
    }

    public void setDataIndexMapOffsets(long[] dataIndexMapOffsets) {
        this.dataIndexMapOffsets = dataIndexMapOffsets;
    }

    public boolean[] getAggKeyBlock() {
        return aggKeyBlock;
    }

    public void setAggKeyBlock(boolean[] aggKeyBlock) {
        this.aggKeyBlock = aggKeyBlock;
    }

    public NumberCompressor[] getKeyBlockUnCompressor() {
        return keyBlockUnCompressor;
    }

    public void setKeyBlockUnCompressor(NumberCompressor[] keyBlockUnCompressor) {
        this.keyBlockUnCompressor = keyBlockUnCompressor;
    }

    /**
     * for each column min max data
     */
    public byte[][] getColumnMinMaxData() {
        return this.columnMinMaxData;
    }

    /**
     * for each column min max data
     */
    public void setColumnMinMaxData(byte[][] columnMinMaxData) {
        this.columnMinMaxData = columnMinMaxData;

    }

}
