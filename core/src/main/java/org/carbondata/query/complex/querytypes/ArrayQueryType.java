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

package org.carbondata.query.complex.querytypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.datastorage.InMemoryTable;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.GenericArrayData;

public class ArrayQueryType implements GenericQueryType {

  private GenericQueryType children;

  private String name;

  private String parentname;

  private int blockIndex;

  private int keyOrdinalForQuery;

  public ArrayQueryType(String name, String parentname, int blockIndex) {
    this.name = name;
    this.parentname = parentname;
    this.blockIndex = blockIndex;
  }

  @Override public void addChildren(GenericQueryType children) {
    if (this.getName().equals(children.getParentname())) {
      this.children = children;
    } else {
      this.children.addChildren(children);
    }
  }

  @Override public String getName() {
    return name;
  }

  @Override public void setName(String name) {
    this.name = name;
  }

  @Override public String getParentname() {
    return parentname;
  }

  @Override public void setParentname(String parentname) {
    this.parentname = parentname;

  }

  @Override public void getAllPrimitiveChildren(List<GenericQueryType> primitiveChild) {
    if (children instanceof PrimitiveQueryType) {
      primitiveChild.add(children);
    } else {
      children.getAllPrimitiveChildren(primitiveChild);
    }
  }

  @Override public int getSurrogateIndex() {
    return 0;
  }

  @Override public void setSurrogateIndex(int surrIndex) {

  }

  @Override public int getBlockIndex() {
    return blockIndex;
  }

  @Override public void setBlockIndex(int blockIndex) {
    this.blockIndex = blockIndex;
  }

  @Override public int getColsCount() {
    return children.getColsCount() + 1;
  }

  @Override public void parseBlocksAndReturnComplexColumnByteArray(
      ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder, int rowNumber,
      DataOutputStream dataOutputStream) throws IOException {
    byte[] input = new byte[8];
    if (!columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().isSorted()) {
      System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(),
          columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
              .getColumnReverseIndex()[rowNumber] * columnarKeyStoreDataHolder[blockIndex]
              .getColumnarKeyStoreMetadata().getEachRowSize(), input, 0,
          columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().getEachRowSize());
    } else {

      System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(),
          rowNumber * columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
              .getEachRowSize(), input, 0,
          columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().getEachRowSize());
    }

    ByteBuffer byteArray = ByteBuffer.wrap(input);
    int dataLength = byteArray.getInt();
    dataOutputStream.writeInt(dataLength);
    if (dataLength == 0) {
      //            b.putInt(0);
    } else {
      int columnIndex = byteArray.getInt();
      for (int i = 0; i < dataLength; i++) {
        children
            .parseBlocksAndReturnComplexColumnByteArray(columnarKeyStoreDataHolder, columnIndex++,
                dataOutputStream);
      }
    }
  }

  @Override public void parseAndGetResultBytes(ByteBuffer complexData, DataOutputStream dataOutput)
      throws IOException {
    int dataLength = complexData.getInt();
    dataOutput.writeInt(dataLength);
    for (int i = 0; i < dataLength; i++) {
      children.parseAndGetResultBytes(complexData, dataOutput);
    }
  }

  @Override public void setKeySize(int[] keyBlockSize) {
    children.setKeySize(keyBlockSize);
  }

  @Override public Object getDataBasedOnDataTypeFromSurrogates(List<InMemoryTable> slices,
      ByteBuffer surrogateData, Dimension[] dimensions) {
    int dataLength = surrogateData.getInt();
    if (dataLength == -1) {
      return null;
    }
    Object[] data = new Object[dataLength];
    for (int i = 0; i < dataLength; i++) {
      data[i] = children.getDataBasedOnDataTypeFromSurrogates(slices, surrogateData, dimensions);
    }
    return new GenericArrayData(data);
  }

  @Override public DataType getSchemaType() {
    return new ArrayType(children.getSchemaType(), true);
  }

  @Override public int getKeyOrdinalForQuery() {
    return keyOrdinalForQuery;
  }

  @Override public void setKeyOrdinalForQuery(int keyOrdinalForQuery) {
    this.keyOrdinalForQuery = keyOrdinalForQuery;
  }

  @Override public void fillRequiredBlockData(BlocksChunkHolder blockChunkHolder) {
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    children.fillRequiredBlockData(blockChunkHolder);
  }

}
