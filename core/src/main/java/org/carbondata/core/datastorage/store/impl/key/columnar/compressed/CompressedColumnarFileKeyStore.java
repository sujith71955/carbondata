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

package org.carbondata.core.datastorage.store.impl.key.columnar.compressed;

import java.util.List;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import org.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.carbondata.core.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import org.carbondata.core.util.CarbonUtil;

public class CompressedColumnarFileKeyStore extends AbstractColumnarKeyStore {

    public CompressedColumnarFileKeyStore(ColumnarKeyStoreInfo columnarStoreInfo) {
        super(columnarStoreInfo, false, null);
    }

    @Override
    public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(FileHolder fileHolder,
            int[] blockIndex, boolean[] needCompressedData,int[] directSurrogates) {
        ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolders =
                new ColumnarKeyStoreDataHolder[blockIndex.length];

        for (int i = 0; i < columnarKeyStoreDataHolders.length; i++) {
            byte[] columnarKeyBlockData = null;
            int[] columnKeyBlockIndex = null;
            int[] columnKeyBlockReverseIndexes = null;
            ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
            int columnarKeyBlockIndex = 0;
            int[] dataIndex = null;
            boolean isUnCompressed = true;
            columnarKeyBlockData = COMPRESSOR.unCompress(fileHolder
                    .readByteArray(columnarStoreInfo.getFilePath(),
                            columnarStoreInfo.getKeyBlockOffsets()[blockIndex[i]],
                            columnarStoreInfo.getKeyBlockLengths()[blockIndex[i]]));
            boolean isHighCardinalityBlock=CompressedColumnarKeyStoreUtil.isHighCardinalityBlock(directSurrogates, blockIndex[i]);
            if (!isHighCardinalityBlock
                    && this.columnarStoreInfo.getAggKeyBlock()[blockIndex[i]]) {
                dataIndex = columnarStoreInfo.getNumberCompressor().unCompress(fileHolder
                        .readByteArray(columnarStoreInfo.getFilePath(),
                                columnarStoreInfo.getDataIndexMapOffsets()[mapOfAggDataIndex
                                        .get(blockIndex[i])],
                                columnarStoreInfo.getDataIndexMapLength()[mapOfAggDataIndex
                                        .get(blockIndex[i])]));
                if (!needCompressedData[i]) {
                    columnarKeyBlockData = UnBlockIndexer
                            .uncompressData(columnarKeyBlockData, dataIndex,
                                    columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
                    dataIndex = null;
                } else {
                    isUnCompressed = false;
                }
            }
            if (!columnarStoreInfo.getIsSorted()[blockIndex[i]]) {
                columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[i]);
                columnKeyBlockIndex = CarbonUtil.getUnCompressColumnIndex(
                        columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
                        fileHolder.readByteArray(columnarStoreInfo.getFilePath(),
                                columnarStoreInfo.getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                                columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex]),
                        columnarStoreInfo.getNumberCompressor());
                columnKeyBlockReverseIndexes = getColumnIndexForNonFilter(columnKeyBlockIndex);
            }
            //Since its an high cardinality dimension adding the direct surrogates as part of
            //columnarKeyStoreMetadata so that later it will be used with bytearraywrapper instance.
            if (isHighCardinalityBlock) {
                columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
                columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
                columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndexes);
                columnarKeyStoreMetadata.setDirectSurrogateColumn(true);
                columnarKeyStoreMetadata.setUnCompressed(true);
                columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
                //System is reading the direct surrogates data from byte array which contains both length and the
                //direct surrogates data
                List<byte[]> directSurrogateBasedKeyBlockData= CompressedColumnarKeyStoreUtil.readColumnarKeyBlockDataForNoDictionaryCols(columnarKeyBlockData);
                columnarKeyStoreDataHolders[i] =
                        new ColumnarKeyStoreDataHolder(directSurrogateBasedKeyBlockData,
                                columnarKeyStoreMetadata);
            } else {
                columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(
                        columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
                columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
                columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
                columnarKeyStoreMetadata.setDataIndex(dataIndex);
                columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndexes);
                columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
                columnarKeyStoreDataHolders[i] =
                        new ColumnarKeyStoreDataHolder(columnarKeyBlockData,
                                columnarKeyStoreMetadata);
            }
        }
        return columnarKeyStoreDataHolders;
    }

    @Override
    public ColumnarKeyStoreDataHolder getUnCompressedKeyArray(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData,int[] directSurrogates) {
        byte[] columnarKeyBlockData = null;
        int[] columnKeyBlockIndex = null;
        int[] columnKeyBlockReverseIndex = null;
        ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
        int columnarKeyBlockIndex = 0;
        int[] dataIndex = null;
        boolean isUnCompressed = true;
        columnarKeyBlockData = COMPRESSOR.unCompress(fileHolder
                .readByteArray(columnarStoreInfo.getFilePath(),
                        columnarStoreInfo.getKeyBlockOffsets()[blockIndex],
                        columnarStoreInfo.getKeyBlockLengths()[blockIndex]));
        boolean isHighCardinalityBlock=CompressedColumnarKeyStoreUtil.isHighCardinalityBlock(directSurrogates, blockIndex);
        if (!isHighCardinalityBlock
                && this.columnarStoreInfo.getAggKeyBlock()[blockIndex]) {
            dataIndex = columnarStoreInfo.getNumberCompressor().unCompress(fileHolder
                    .readByteArray(columnarStoreInfo.getFilePath(),
                            columnarStoreInfo.getDataIndexMapOffsets()[mapOfAggDataIndex
                                    .get(blockIndex)],
                            columnarStoreInfo.getDataIndexMapLength()[mapOfAggDataIndex
                                    .get(blockIndex)]));
            if (!needCompressedData) {
                columnarKeyBlockData = UnBlockIndexer
                        .uncompressData(columnarKeyBlockData, dataIndex,
                                columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
                dataIndex = null;
            } else {
                isUnCompressed = false;
            }
        }
        if (!columnarStoreInfo.getIsSorted()[blockIndex]) {
            columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex);
            columnKeyBlockIndex = CarbonUtil.getUnCompressColumnIndex(
                    columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex], fileHolder
                            .readByteArray(columnarStoreInfo.getFilePath(), columnarStoreInfo
                                            .getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                                    columnarStoreInfo
                                            .getKeyBlockIndexLength()[columnarKeyBlockIndex]),
                    columnarStoreInfo.getNumberCompressor());
            columnKeyBlockReverseIndex = getColumnIndexForNonFilter(columnKeyBlockIndex);
        }
        //Since its an high cardinality dimension, For filter queries.
      if (isHighCardinalityBlock) {
      columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
      ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders =
          CompressedColumnarKeyStoreUtil.createColumnarKeyStoreMetadataForHCDims(blockIndex,
            columnarKeyBlockData, columnKeyBlockIndex, columnKeyBlockReverseIndex,
            columnarStoreInfo);
      new ColumnarKeyStoreDataHolder(columnarKeyBlockData, columnarKeyStoreMetadata);
      return columnarKeyStoreDataHolders;
      }
        columnarKeyStoreMetadata =
                new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
        columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
        columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
        columnarKeyStoreMetadata.setDataIndex(dataIndex);
        columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
        columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
        if (columnarStoreInfo.getHybridStoreModel().isHybridStore() && blockIndex == 0) {
            columnarKeyStoreMetadata.setRowStore(true);
        }
        ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders =
                new ColumnarKeyStoreDataHolder(columnarKeyBlockData, columnarKeyStoreMetadata);
        return columnarKeyStoreDataHolders;
    }

}
