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

package org.carbondata.query.columnar.aggregator.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.util.AggUtil;
import org.carbondata.query.columnar.aggregator.ColumnarAggregatorInfo;
import org.carbondata.query.columnar.aggregator.ColumnarScannedResultAggregator;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;
import org.carbondata.query.executer.impl.RestructureHolder;
import org.carbondata.query.result.Result;
import org.carbondata.query.result.impl.ListBasedResult;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.QueryExecutorUtility;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class ListBasedResultAggregatorImpl implements ColumnarScannedResultAggregator {
    /**
     * LOGGER.
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ListBasedResultAggregatorImpl.class.getName());

    private List<ByteArrayWrapper> keys;

    private List<MeasureAggregator[]> values;

    private ColumnarAggregatorInfo columnaraggreagtorInfo;

    private DataAggregator dataAggregator;

    private boolean isAggTable;

    private int rowCounter;

    private int limit;

    public ListBasedResultAggregatorImpl(ColumnarAggregatorInfo columnaraggreagtorInfo,
            DataAggregator dataAggregator) {
        this.columnaraggreagtorInfo = columnaraggreagtorInfo;
        this.dataAggregator = dataAggregator;
        isAggTable = columnaraggreagtorInfo.getCountMsrIndex() > -1;
        limit = columnaraggreagtorInfo.getLimit();
    }

    @Override
    public int aggregateData(AbstractColumnarScanResult keyValue) {
        this.keys = new ArrayList<ByteArrayWrapper>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        this.values =
                new ArrayList<MeasureAggregator[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        ByteArrayWrapper key = null;
        MeasureAggregator[] value = null;
        while (keyValue.hasNext() && (limit == -1 || rowCounter < limit)) {
            key = new ByteArrayWrapper();
            //Primitives types selected
            if (columnaraggreagtorInfo.getQueryDimensionsLength() == keyValue.getKeyBlockLength()) {
                key.setMaskedKey(keyValue.getKeyArray(key));
            } else {
                //Complex columns selected.
                List<byte[]> complexKeyArray = keyValue.getKeyArrayWithComplexTypes(
                        this.columnaraggreagtorInfo.getComplexQueryDims(), key);
                key.setMaskedKey(complexKeyArray.remove(complexKeyArray.size() - 1));
                for (byte[] complexKey : complexKeyArray) {
                    key.addComplexTypeData(complexKey);
                }
            }
            value = AggUtil.getAggregators(columnaraggreagtorInfo.getAggType(), isAggTable, null,
                    columnaraggreagtorInfo.getCubeUniqueName(),
                    columnaraggreagtorInfo.getMsrMinValue(),
                    columnaraggreagtorInfo.getHighCardinalityTypes(),
                    columnaraggreagtorInfo.getDataTypes());
            dataAggregator.aggregateData(keyValue, value, key);
            keys.add(key);
            values.add(value);
            rowCounter++;
        }
        return rowCounter;
    }

    @Override
    public Result getResult(RestructureHolder restructureHolder) {
        List<ByteArrayWrapper> finalKeys =
                new ArrayList<ByteArrayWrapper>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<MeasureAggregator[]> finalValues =
                new ArrayList<MeasureAggregator[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        Result<List<ByteArrayWrapper>, List<MeasureAggregator[]>> result = new ListBasedResult();

        if (!restructureHolder.updateRequired) {
            result.addScannedResult(keys, values);
        } else {
            updateScannedResult(restructureHolder, finalKeys, finalValues);
            result.addScannedResult(finalKeys, finalValues);
        }
        return result;
    }

    private void updateScannedResult(RestructureHolder restructureHolder,
            List<ByteArrayWrapper> finalKeys, List<MeasureAggregator[]> finalValues) {
        if (!restructureHolder.updateRequired) {
            return;
        }

        try {
            long[] data = null;
            long[] updatedData = null;
            ByteArrayWrapper key = null;
            for (int i = 0; i < keys.size(); i++) {
                key = keys.get(i);
                data = restructureHolder.getKeyGenerator()
                        .getKeyArray(key.getMaskedKey(), restructureHolder.maskedByteRanges);
                updatedData =
                        new long[columnaraggreagtorInfo.getLatestKeyGenerator().getDimCount()];
                Arrays.fill(updatedData, 1);
                System.arraycopy(data, 0, updatedData, 0, data.length);
                if (restructureHolder.metaData.getNewDimsDefVals() != null
                        && restructureHolder.metaData.getNewDimsDefVals().length > 0) {
                    for (int k = 0;
                         k < restructureHolder.metaData.getNewDimsDefVals().length; k++) {
                      if(restructureHolder.getIsHighCardinalityNewDims()[k])
                      {
                        key.addToDirectSurrogateKeyList(restructureHolder.metaData.getNewDimsDefVals()[k].getBytes());
                      }
                      else
                      {
                        updatedData[data.length + k] =
                                restructureHolder.metaData.getNewDimsSurrogateKeys()[k];
                      }
                    }
                }
                if (restructureHolder.getQueryDimsCount() == columnaraggreagtorInfo
                        .getLatestKeyGenerator().getDimCount()) {
                    key.setMaskedKey(QueryExecutorUtility.getMaskedKey(
                            columnaraggreagtorInfo.getLatestKeyGenerator().generateKey(updatedData),
                            columnaraggreagtorInfo.getActualMaxKeyBasedOnDimensions(),
                            columnaraggreagtorInfo.getActalMaskedByteRanges(),
                            columnaraggreagtorInfo.getActualMaskedKeyByteSize()));
                }
                finalKeys.add(key);
                finalValues.add(values.get(i));
            }
        } catch (KeyGenException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
        }
    }
}
