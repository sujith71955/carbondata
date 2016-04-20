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
package org.carbondata.query.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.carbondata.core.carbon.metadata.datatype.ConvertedType;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.leafnode.DataFileMetadata;
import org.carbondata.core.carbon.metadata.leafnode.LeafNodeInfo;
import org.carbondata.core.carbon.metadata.leafnode.SegmentInfo;
import org.carbondata.core.carbon.metadata.leafnode.compressor.ChunkCompressorMeta;
import org.carbondata.core.carbon.metadata.leafnode.compressor.CompressionCodec;
import org.carbondata.core.carbon.metadata.leafnode.datachunk.DataChunk;
import org.carbondata.core.carbon.metadata.leafnode.datachunk.PresenceMeta;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeBtreeIndex;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeIndex;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeMinMaxIndex;
import org.carbondata.core.carbon.metadata.leafnode.sort.SortState;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.metadata.ValueEncoderMeta;
import org.carbondata.core.reader.CarbonMetaDataReader;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.format.BlockletBTreeIndex;
import org.carbondata.format.FileMeta;

/**
 * Below class will be used to convert the thrift object of data file
 * meta data to wrapper object
 */
public class DataFileMetadataConverter {

    /**
     * Below method will be used to get thrift file meta to wrapper file meta
     *
     * @param meta data file path
     *             thrift
     * @return wrapper file meta
     * @throws IOException throw exception if any problem in reading
     */
    public DataFileMetadata readDataFileMetadata(String matadataFilePath, long offset)
            throws IOException {
        CarbonMetaDataReader reader = new CarbonMetaDataReader(matadataFilePath, offset);
        FileMeta fileMeta = reader.readMetaData();
        DataFileMetadata dataFileMetadata = new DataFileMetadata();
        dataFileMetadata.setVersionId(fileMeta.getVersion());
        dataFileMetadata.setNumberOfRows(fileMeta.getNum_rows());
        dataFileMetadata.setSegmentInfo(getSegmentInfo(fileMeta.getSegment_info()));
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        for (int i = 0; i < columnSchemaList.size(); i++) {
            columnSchemaList.add(thriftColumnSchmeaToWrapperColumnSchema(
                    fileMeta.getTable_columns().get(i)));
        }
        dataFileMetadata.setColumnInTable(columnSchemaList);
        List<LeafNodeIndex> leafNodeIndexList = getLeafNodeIndexList(fileMeta.getIndex());
        List<org.carbondata.format.BlockletInfo> leaf_node_infos_Thrift =
                fileMeta.getLeaf_node_info();
        List<LeafNodeInfo> leafNodeInfoList = new ArrayList<LeafNodeInfo>();
        for (int i = 0; i < leaf_node_infos_Thrift.size(); i++) {
            LeafNodeInfo leafNodeInfo = getLeafNodeInfo(leaf_node_infos_Thrift.get(i));
            leafNodeInfo.setLeafNodeIndex(leafNodeIndexList.get(i));
            leafNodeInfoList.add(leafNodeInfo);
        }
        dataFileMetadata.setLeafNodeIndex(getLeafNodeIndexForDataFileMetadata(leafNodeIndexList));
        return dataFileMetadata;
    }

    /**
     * Below method will be used to get leaf node index for data file meta
     *
     * @param leafNodeIndexList
     * @return leaf node index
     */
    private LeafNodeIndex getLeafNodeIndexForDataFileMetadata(
            List<LeafNodeIndex> leafNodeIndexList) {
        LeafNodeIndex leafNodeIndex = new LeafNodeIndex();
        org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeBtreeIndex leafNodeBTreeIndex =
                new LeafNodeBtreeIndex();
        leafNodeBTreeIndex.setStartKey(leafNodeIndexList.get(0).getBtreeIndex().getStartKey());
        leafNodeBTreeIndex.setEndKey(
                leafNodeIndexList.get(leafNodeIndexList.size() - 1).getBtreeIndex().getEndKey());
        leafNodeIndex.setBtreeIndex(leafNodeBTreeIndex);
        byte[][] currentMinValue = leafNodeIndexList.get(0).getMinMaxIndex().getMinValues().clone();
        byte[][] currentMaxValue = leafNodeIndexList.get(0).getMinMaxIndex().getMaxValues().clone();
        byte[][] minValue = null;
        byte[][] maxValue = null;
        for (int i = 1; i < leafNodeIndexList.size(); i++) {
            minValue = leafNodeIndexList.get(i).getMinMaxIndex().getMinValues();
            maxValue = leafNodeIndexList.get(i).getMinMaxIndex().getMaxValues();
            for (int j = 0; j < maxValue.length; j++) {
                if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMinValue[i], minValue[i])
                        < 0) {
                    currentMinValue[i] = minValue[i].clone();
                }
                if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(currentMaxValue[i], maxValue[i])
                        < 0) {
                    currentMaxValue[i] = maxValue[i].clone();
                }
            }
        }

        LeafNodeMinMaxIndex minMax = new LeafNodeMinMaxIndex();
        minMax.setMaxValues(currentMaxValue);
        minMax.setMinValues(currentMinValue);
        leafNodeIndex.setMinMaxIndex(minMax);
        return leafNodeIndex;
    }

    private ColumnSchema thriftColumnSchmeaToWrapperColumnSchema(
            org.carbondata.format.ColumnSchema externalColumnSchema) {
        ColumnSchema wrapperColumnSchema = new ColumnSchema();
        wrapperColumnSchema.setColumnUniqueId(externalColumnSchema.getColumn_id());
        wrapperColumnSchema.setColumnName(externalColumnSchema.getColumn_name());
        wrapperColumnSchema.setColumnar(externalColumnSchema.isColumnar());
        wrapperColumnSchema
                .setDataType(thriftDataTyopeToWrapperDataType(externalColumnSchema.data_type));
        wrapperColumnSchema.setDimensionColumn(externalColumnSchema.isDimension());
        List<Encoding> encoders = new ArrayList<Encoding>();
        for (org.carbondata.format.Encoding encoder : externalColumnSchema.getEncoders()) {
            encoders.add(fromExternalToWrapperEncoding(encoder));
        }
        wrapperColumnSchema.setEncodintList(encoders);
        wrapperColumnSchema.setConvertedType(thriftConvertedTypeToWrapperConvertedTypeConverter(
                externalColumnSchema.getConverted_type()));
        wrapperColumnSchema.setNumberOfChild(externalColumnSchema.getNum_child());
        wrapperColumnSchema.setPrecision(externalColumnSchema.getPrecision());
        wrapperColumnSchema.setRowGroupId(externalColumnSchema.getColumn_group_id());
        wrapperColumnSchema.setScale(externalColumnSchema.getScale());
        wrapperColumnSchema.setDefaultValue(externalColumnSchema.getDefault_value());
        wrapperColumnSchema.setAggregateFunction(externalColumnSchema.getAggregate_function());
        return wrapperColumnSchema;
    }

    /**
     * Below method is to convert the leaf node info of the thrift to wrapper
     * leaf node info
     *
     * @param leafNodeInfoThrift leaf node info of the thrift
     * @return leaf node info wrapper
     */
    private LeafNodeInfo getLeafNodeInfo(org.carbondata.format.BlockletInfo leafNodeInfoThrift) {
        LeafNodeInfo leafNodeInfo = new LeafNodeInfo();
        List<DataChunk> dimensionColumnChunk = new ArrayList<DataChunk>();
        List<DataChunk> measureChunk = new ArrayList<DataChunk>();
        Iterator<org.carbondata.format.DataChunk> column_data_chunksIterator =
                leafNodeInfoThrift.getColumn_data_chunksIterator();
        while (column_data_chunksIterator.hasNext()) {
            org.carbondata.format.DataChunk next = column_data_chunksIterator.next();
            if (next.isRow_chunk()) {
                dimensionColumnChunk.add(getDataChunk(next));
            } else if (next.getEncoders().contains(org.carbondata.format.Encoding.DELTA)) {
                measureChunk.add(getDataChunk(next));
            } else {

                dimensionColumnChunk.add(getDataChunk(next));
            }
        }
        leafNodeInfo.setDimensionColumnChunk(dimensionColumnChunk);
        leafNodeInfo.setMeasureColumnChunk(measureChunk);
        leafNodeInfo.setNumberOfRows(leafNodeInfoThrift.getColumn_data_chunksSize());
        return leafNodeInfo;
    }

    /**
     * Below method is convert the thrift encoding to wrapper encoding
     *
     * @param encoderThrift thrift encoding
     * @return wrapper encoding
     */
    private Encoding fromExternalToWrapperEncoding(org.carbondata.format.Encoding encoderThrift) {
        switch (encoderThrift) {
            case DICTIONARY:
                return Encoding.DICTIONARY;
            case DELTA:
                return Encoding.DELTA;
            case RLE:
                return Encoding.RLE;
            case INVERTED_INDEX:
                return Encoding.INVERTED_INDEX;
            case BIT_PACKED:
                return Encoding.BIT_PACKED;
            default:
                return Encoding.DICTIONARY;
        }
    }

    /**
     * Below method will be used to convert the thrift compression to wrapper
     * compression codec
     *
     * @param compressionCodecThrift
     * @return wrapper compression codec
     */
    private CompressionCodec getCompressionCodec(
            org.carbondata.format.CompressionCodec compressionCodecThrift) {
        switch (compressionCodecThrift) {
            case SNAPPY:
                return CompressionCodec.SNAPPY;
            default:
                return CompressionCodec.SNAPPY;
        }
    }

    /**
     * Below method will be used to convert thrift segment object to wrapper
     * segment object
     *
     * @param segmentInfo thrift segment info object
     * @return wrapper segment info object
     */
    private SegmentInfo getSegmentInfo(org.carbondata.format.SegmentInfo segmentInfo) {
        SegmentInfo info = new SegmentInfo();
        int[] cardinality = new int[segmentInfo.getColumn_cardinalities().size()];
        for (int i = 0; i < cardinality.length; i++) {
            cardinality[i] = segmentInfo.getColumn_cardinalities().get(i);
        }
        info.setNumberOfColumns(segmentInfo.getNum_cols());
        return info;
    }

    /**
     * Below method will be used to convert the leaf node index of thrift to
     * wrapper
     *
     * @param leafNodeIndexThrift
     * @return leaf node index wrapper
     */
    private List<LeafNodeIndex> getLeafNodeIndexList(
            org.carbondata.format.BlockletIndex leafNodeIndexThrift) {

        List<BlockletBTreeIndex> thriftBtreeIndexList = leafNodeIndexThrift.getB_tree_index();
        List<org.carbondata.format.BlockletMinMaxIndex> min_max_index =
                leafNodeIndexThrift.getMin_max_index();
        List<LeafNodeIndex> leafNodeIndex =
                new ArrayList<LeafNodeIndex>(thriftBtreeIndexList.size());
        LeafNodeBtreeIndex btreeIndex = null;
        BlockletBTreeIndex leafNodeBTreeIndex = null;
        LeafNodeIndex indexInfo = null;
        LeafNodeMinMaxIndex minMaxIndex = null;
        for (int i = 0; i < thriftBtreeIndexList.size(); i++) {
            indexInfo = null;
            btreeIndex = new LeafNodeBtreeIndex();
            minMaxIndex = new LeafNodeMinMaxIndex();
            leafNodeBTreeIndex = thriftBtreeIndexList.get(i);
            List<ByteBuffer> max_values = min_max_index.get(i).getMax_values();
            List<ByteBuffer> min_values = min_max_index.get(i).getMin_values();
            byte[][] minValues = new byte[max_values.size()][];
            byte[][] maxValues = new byte[min_values.size()][];
            for (int j = 0; j < maxValues.length; j++) {
                minValues[i] = min_values.get(i).array();
                maxValues[i] = max_values.get(i).array();
            }
            indexInfo = new LeafNodeIndex();
            btreeIndex.setStartKey(leafNodeBTreeIndex.getStart_key());
            btreeIndex.setEndKey(leafNodeBTreeIndex.getEnd_key());
            indexInfo.setBtreeIndex(btreeIndex);
            minMaxIndex.setMaxValues(maxValues);
            minMaxIndex.setMinValues(minValues);
            indexInfo.setBtreeIndex(btreeIndex);
            indexInfo.setMinMaxIndex(minMaxIndex);
            leafNodeIndex.add(indexInfo);
        }
        return leafNodeIndex;
    }

    /**
     * Below method will be used to convert the thrift compression meta to
     * wrapper chunk compression meta
     *
     * @param chunkCompressionMetaThrift
     * @return chunkCompressionMetaWrapper
     */
    private ChunkCompressorMeta getChunkCompressionMeta(
            org.carbondata.format.ChunkCompressionMeta chunkCompressionMetaThrift) {
        ChunkCompressorMeta compressorMeta = new ChunkCompressorMeta();
        compressorMeta.setCompressor(
                getCompressionCodec(chunkCompressionMetaThrift.getCompression_codec()));
        compressorMeta.setCompressedSize(chunkCompressionMetaThrift.getTotal_compressed_size());
        compressorMeta.setUncompressedSize(chunkCompressionMetaThrift.getTotal_uncompressed_size());
        return compressorMeta;
    }

    /**
     * Below method will be used to convert the thrift data type to wrapper data
     * type
     *
     * @param dataTypeThrift
     * @return dataType wrapper
     */
    private DataType thriftDataTyopeToWrapperDataType(
            org.carbondata.format.DataType dataTypeThrift) {
        switch (dataTypeThrift) {
            case STRING:
                return DataType.STRING;
            case INT:
                return DataType.INT;
            case LONG:
                return DataType.LONG;
            case DOUBLE:
                return DataType.DOUBLE;
            case DECIMAL:
                return DataType.DECIMAL;
            case TIMESTAMP:
                return DataType.TIMESTAMP;
            case ARRAY:
                return DataType.ARRAY;
            case STRUCT:
                return DataType.STRUCT;
            default:
                return DataType.STRING;
        }
    }

    /**
     * Below method is to convert the thrift converted type to wrapper converted
     * type
     *
     * @param convertedType thrift
     * @return wrapper converted type
     */
    private ConvertedType thriftConvertedTypeToWrapperConvertedTypeConverter(
            org.carbondata.format.ConvertedType convertedType) {
        switch (convertedType) {
            case UTF8:
                return ConvertedType.UTF8;
            case MAP:
                return ConvertedType.MAP;
            case MAP_KEY_VALUE:
                return ConvertedType.MAP_KEY_VALUE;
            case LIST:
                return ConvertedType.LIST;
            case ENUM:
                return ConvertedType.ENUM;
            case DECIMAL:
                return ConvertedType.DECIMAL;
            case DATE:
                return ConvertedType.DATE;
            case TIME_MILLIS:
                return ConvertedType.TIME_MILLIS;
            case TIMESTAMP_MILLIS:
                return ConvertedType.TIMESTAMP_MILLIS;
            case RESERVED:
                return ConvertedType.RESERVED;
            case UINT_8:
                return ConvertedType.UINT_8;
            case UINT_16:
                return ConvertedType.UINT_16;
            case UINT_32:
                return ConvertedType.UINT_32;
            case UINT_64:
                return ConvertedType.UINT_64;
            case INT_8:
                return ConvertedType.INT_8;
            case INT_16:
                return ConvertedType.INT_16;
            case INT_32:
                return ConvertedType.INT_32;
            case INT_64:
                return ConvertedType.INT_64;
            case JSON:
                return ConvertedType.JSON;
            case BSON:
                return ConvertedType.BSON;
            case INTERVAL:
                return ConvertedType.INTERVAL;
            default:
                return ConvertedType.UTF8;
        }
    }

    /**
     * Below method will be used to convert the thrift presence meta to wrapper
     * presence meta
     *
     * @param presentMetadataThrift
     * @return wrapper presence meta
     */
    private PresenceMeta getPresenceMeta(org.carbondata.format.PresenceMeta presentMetadataThrift) {
        PresenceMeta presenceMeta = new PresenceMeta();
        presenceMeta.setRepresentNullValues(presentMetadataThrift.isRepresents_presence());
        presenceMeta.setBitSet(BitSet.valueOf(presentMetadataThrift.getPresent_bit_stream()));
        return presenceMeta;
    }

    /**
     * Below method will be used to convert the thrift object to wrapper object
     *
     * @param sortStateThrift
     * @return wrapper sort state object
     */
    private SortState getSortState(org.carbondata.format.SortState sortStateThrift) {
        if (sortStateThrift == org.carbondata.format.SortState.SORT_EXPLICIT) {
            return SortState.SORT_EXPLICT;
        } else if (sortStateThrift == org.carbondata.format.SortState.SORT_NATIVE) {
            return SortState.SORT_NATIVE;
        } else {
            return SortState.SORT_NONE;
        }
    }

    /**
     * Below method will be used to convert the thrift data chunk to wrapper
     * data chunk
     *
     * @param datachunkThrift
     * @return wrapper data chunk
     */
    private DataChunk getDataChunk(org.carbondata.format.DataChunk datachunkThrift) {
        DataChunk dataChunk = new DataChunk();
        dataChunk.setColumnUniqueIdList(datachunkThrift.getColumn_ids());
        dataChunk.setDataPageLength(datachunkThrift.getData_page_length());
        dataChunk.setNullValueIndexForColumn(getPresenceMeta(datachunkThrift.getPresence()));
        dataChunk.setRlePageLength(datachunkThrift.getRle_page_length());
        dataChunk.setRlePageOffset(datachunkThrift.getRle_page_offset());
        dataChunk.setRowChunk(datachunkThrift.isRow_chunk());
        dataChunk.setRowIdPageLength(datachunkThrift.getRowid_page_length());
        dataChunk.setRowIdPageOffset(datachunkThrift.getRowid_page_offset());
        dataChunk.setSortState(getSortState(datachunkThrift.getSort_state()));
        dataChunk.setChunkCompressionMeta(getChunkCompressionMeta(datachunkThrift.getChunk_meta()));
        List<Encoding> encodingList = new ArrayList<Encoding>(datachunkThrift.getEncoders().size());
        for (int i = 0; i < datachunkThrift.getEncoders().size(); i++) {
            encodingList.add(fromExternalToWrapperEncoding(datachunkThrift.getEncoders().get(i)));
        }
        dataChunk.setEncoderList(encodingList);
        if (encodingList.contains(Encoding.DELTA)) {
            List<ValueEncoderMeta> encodeMetaList =
                    new ArrayList<ValueEncoderMeta>(datachunkThrift.getEncoder_meta().size());
            for (int i = 0; i < encodeMetaList.size(); i++) {
                encodeMetaList.add(deserializeEncoderMeta(
                        datachunkThrift.getEncoder_meta().get(i).array()));
            }
            dataChunk.setValueEncoderMeta(encodeMetaList);
        }
        return dataChunk;
    }

    /**
     * Below method will be used to convert the encode metadata to
     * ValueEncoderMeta object
     *
     * @param encoderMeta
     * @return ValueEncoderMeta object
     */
    private ValueEncoderMeta deserializeEncoderMeta(byte[] encoderMeta) {
        // TODO : should remove the unnecessary fields.
        ByteArrayInputStream aos = null;
        ObjectInputStream objStream = null;
        ValueEncoderMeta meta = null;
        try {
            aos = new ByteArrayInputStream(encoderMeta);
            objStream = new ObjectInputStream(aos);
            meta = (ValueEncoderMeta) objStream.readObject();
        } catch (ClassNotFoundException e) {

        } catch (IOException e) {
            CarbonUtil.closeStreams(objStream);
        }
        return meta;
    }
}
