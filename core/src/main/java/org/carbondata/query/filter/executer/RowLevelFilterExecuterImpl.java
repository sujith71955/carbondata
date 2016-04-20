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
package org.carbondata.query.filter.executer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.util.AggUtil;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.evaluators.DimColumnEvaluatorInfo;
import org.carbondata.query.evaluators.FilterProcessorPlaceHolder;
import org.carbondata.query.evaluators.MsrColumnEvalutorInfo;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.DataTypeConverter;
import org.carbondata.query.util.QueryExecutorUtility;

public class RowLevelFilterExecuterImpl implements FilterExecuter {

	private List<DimColumnEvaluatorInfo> dimColEvaluatorInfoList;
	private List<MsrColumnEvalutorInfo> msrColEvalutorInfoList;
	private Expression exp;
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(RowLevelFilterExecuterImpl.class.getName());

	public RowLevelFilterExecuterImpl(
			List<DimColumnEvaluatorInfo> dimColEvaluatorInfoList,
			List<MsrColumnEvalutorInfo> msrColEvalutorInfoList, Expression exp) {
		this.dimColEvaluatorInfoList = dimColEvaluatorInfoList;
		this.msrColEvalutorInfoList = msrColEvalutorInfoList;
		this.exp = exp;
	}

	@Override
	public BitSet applyFilter(BlockDataHolder blockDataHolder,
			FilterProcessorPlaceHolder placeHolder, int[] noDictionaryColIndexes) {
		for (DimColumnEvaluatorInfo dimColumnEvaluatorInfo : dimColEvaluatorInfoList) {
			if (dimColumnEvaluatorInfo.getDims().getDataType() != Type.ARRAY
					&& dimColumnEvaluatorInfo.getDims().getDataType() != Type.STRUCT) {
				if (null == blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo
						.getColumnIndex()]) {
					blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo
							.getColumnIndex()] = blockDataHolder
							.getLeafDataBlock().getColumnarKeyStore(
									blockDataHolder.getFileHolder(),
									dimColumnEvaluatorInfo.getColumnIndex(),
									false, noDictionaryColIndexes);
				} else {
					if (!blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo
							.getColumnIndex()].getColumnarKeyStoreMetadata()
							.isUnCompressed()) {
						blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo
								.getColumnIndex()].unCompress();
					}
				}
			} else {
				GenericQueryType complexType = dimColumnEvaluatorInfo
						.getComplexTypesWithBlockStartIndex().get(
								dimColumnEvaluatorInfo.getColumnIndex());
				complexType.fillRequiredBlockData(blockDataHolder);
			}
		}

		// CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
		for (MsrColumnEvalutorInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
			if (msrColumnEvalutorInfo.isMeasureExistsInCurrentSlice()
					&& null == blockDataHolder.getMeasureBlocks()[msrColumnEvalutorInfo
							.getColumnIndex()]) {
				blockDataHolder.getMeasureBlocks()[msrColumnEvalutorInfo
						.getColumnIndex()] = blockDataHolder
						.getLeafDataBlock()
						.getNodeMsrDataWrapper(
								msrColumnEvalutorInfo.getColumnIndex(),
								blockDataHolder.getFileHolder()).getValues()[msrColumnEvalutorInfo
						.getColumnIndex()];
			}
		}
		// CHECKSTYLE:ON

		int numberOfRows = blockDataHolder.getLeafDataBlock().getnKeys();
		BitSet set = new BitSet(numberOfRows);
		RowIntf row = new RowImpl();

		// CHECKSTYLE:OFF Approval No:Approval-V1R2C10_007
		for (int index = 0; index < numberOfRows; index++) {
			createRow(blockDataHolder, row, index);
			try {
				Boolean rslt = exp.evaluate(row).getBoolean();
				if (null != rslt && rslt) {
					set.set(index);
				}
			} catch (FilterUnsupportedException e) {
				LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
						e.getMessage());
			}
		}
		// CHECKSTYLE:ON

		return set;
	}

	private void createRow(BlockDataHolder blockDataHolder, RowIntf row,
			int index) {
		Object[] record = new Object[dimColEvaluatorInfoList.size()
				+ msrColEvalutorInfoList.size()];
		String memberString = null;
		for (DimColumnEvaluatorInfo dimColumnEvaluatorInfo : dimColEvaluatorInfoList) {
			if (dimColumnEvaluatorInfo.getDims().getDataType() != Type.ARRAY
					&& dimColumnEvaluatorInfo.getDims().getDataType() != Type.STRUCT) {
				if (!dimColumnEvaluatorInfo.isDimensionExistsInCurrentSilce()) {
					record[dimColumnEvaluatorInfo.getRowIndex()] = dimColumnEvaluatorInfo
							.getDefaultValue();
				}
				if (dimColumnEvaluatorInfo.getDims().isNoDictionaryDim()) {
					ColumnarKeyStoreDataHolder columnarKeyStoreDataHolder = blockDataHolder
							.getColumnarKeyStore()[dimColumnEvaluatorInfo
							.getColumnIndex()];
					if (null != columnarKeyStoreDataHolder
							.getNoDictionaryValBasedKeyBlockData()) {
						memberString = readMemberBasedOnNoDictionaryVal(
								dimColumnEvaluatorInfo,
								columnarKeyStoreDataHolder, index);
						if (null != memberString) {
							if (memberString
									.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
								memberString = null;
							}
						}
						record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeConverter
								.getDataBasedOnDataType(memberString,
										dimColumnEvaluatorInfo.getDims()
												.getDataType());
					} else {
						continue;
					}
				} else {
					Member member = QueryExecutorUtility
							.getMemberBySurrogateKey(
									dimColumnEvaluatorInfo.getDims(),
									blockDataHolder.getColumnarKeyStore()[dimColumnEvaluatorInfo
											.getColumnIndex()]
											.getSurrogateKey(index),
									dimColumnEvaluatorInfo.getSlices(),
									dimColumnEvaluatorInfo
											.getCurrentSliceIndex());

					if (null != member) {
						memberString = member.toString();
						if (memberString
								.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
							memberString = null;
						}
					}
					record[dimColumnEvaluatorInfo.getRowIndex()] = DataTypeConverter
							.getDataBasedOnDataType(memberString,
									dimColumnEvaluatorInfo.getDims()
											.getDataType());
				}
			} else {
				try {
					GenericQueryType complexType = dimColumnEvaluatorInfo
							.getComplexTypesWithBlockStartIndex().get(
									dimColumnEvaluatorInfo.getColumnIndex());
					ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
					DataOutputStream dataOutputStream = new DataOutputStream(
							byteStream);
					complexType.parseBlocksAndReturnComplexColumnByteArray(
							blockDataHolder.getColumnarKeyStore(), index,
							dataOutputStream);
					record[dimColumnEvaluatorInfo.getRowIndex()] = complexType
							.getDataBasedOnDataTypeFromSurrogates(
									dimColumnEvaluatorInfo.getSlices(),
									ByteBuffer.wrap(byteStream.toByteArray()),
									dimColumnEvaluatorInfo.getDimensions());
					byteStream.close();
				} catch (IOException e) {
					LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
							e.getMessage());
				}

			}
		}

		SqlStatement.Type msrType;

		for (MsrColumnEvalutorInfo msrColumnEvalutorInfo : msrColEvalutorInfoList) {
			switch (msrColumnEvalutorInfo.getType()) {
			case 'l':
				msrType = SqlStatement.Type.LONG;
				break;
			case 'b':
				msrType = SqlStatement.Type.DECIMAL;
				break;
			default:
				msrType = SqlStatement.Type.DOUBLE;
			}
			// if measure doesnt exist then set the default value.
			if (!msrColumnEvalutorInfo.isMeasureExistsInCurrentSlice()) {
				record[msrColumnEvalutorInfo.getRowIndex()] = msrColumnEvalutorInfo
						.getDefaultValue();
			} else {
				if (msrColumnEvalutorInfo.isCustomMeasureValue()) {
					MeasureAggregator aggregator = AggUtil.getAggregator(
							msrColumnEvalutorInfo.getAggregator(), false,
							false, null, false, 0, msrType);
					aggregator
							.merge(blockDataHolder.getMeasureBlocks()[msrColumnEvalutorInfo
									.getColumnIndex()]
									.getReadableByteArrayValueByIndex(index));
					switch (msrType) {
					case LONG:
						record[msrColumnEvalutorInfo.getRowIndex()] = aggregator
								.getLongValue();
						break;
					case DECIMAL:
						record[msrColumnEvalutorInfo.getRowIndex()] = aggregator
								.getBigDecimalValue();
						break;
					default:
						record[msrColumnEvalutorInfo.getRowIndex()] = aggregator
								.getDoubleValue();
					}
				} else {
					Object msrValue;
					switch (msrType) {
					case LONG:
						msrValue = blockDataHolder.getMeasureBlocks()[msrColumnEvalutorInfo
								.getColumnIndex()]
								.getReadableLongValueByIndex(index);
						break;
					case DECIMAL:
						msrValue = blockDataHolder.getMeasureBlocks()[msrColumnEvalutorInfo
								.getColumnIndex()]
								.getReadableBigDecimalValueByIndex(index);
						break;
					default:
						msrValue = blockDataHolder.getMeasureBlocks()[msrColumnEvalutorInfo
								.getColumnIndex()]
								.getReadableDoubleValueByIndex(index);
					}
					if (msrColumnEvalutorInfo.getUniqueValue().equals(msrValue)) {
						record[msrColumnEvalutorInfo.getRowIndex()] = null;
					} else {
						record[msrColumnEvalutorInfo.getRowIndex()] = msrValue;
					}
				}
			}
		}
		row.setValues(record);
	}

	/**
	 * Reading the blocks for direct surrogates.
	 *
	 * @param dimColumnEvaluatorInfo
	 * @param columnarKeyStoreDataHolder
	 * @param index
	 * @return
	 */
	private String readMemberBasedOnNoDictionaryVal(
			DimColumnEvaluatorInfo dimColumnEvaluatorInfo,
			ColumnarKeyStoreDataHolder columnarKeyStoreDataHolder, int index) {
		byte[] noDictionaryVals;
		if (null != columnarKeyStoreDataHolder.getColumnarKeyStoreMetadata()
				.getColumnReverseIndex()) {
			// Getting the data for direct surrogates.
			noDictionaryVals = columnarKeyStoreDataHolder
					.getNoDictionaryValBasedKeyBlockData().get(
							columnarKeyStoreDataHolder
									.getColumnarKeyStoreMetadata()
									.getColumnReverseIndex()[index]);
		} else {
			noDictionaryVals = columnarKeyStoreDataHolder
					.getNoDictionaryValBasedKeyBlockData().get(index);
		}
		return new String(noDictionaryVals);
	}

	@Override
	public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
		BitSet bitSet = new BitSet(1);
		bitSet.set(0);
		return bitSet;
	}

}
