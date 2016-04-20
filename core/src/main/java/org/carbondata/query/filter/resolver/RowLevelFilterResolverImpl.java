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
package org.carbondata.query.filter.resolver;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.query.evaluators.DimColumnEvaluatorInfo;
import org.carbondata.query.evaluators.MsrColumnEvalutorInfo;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filter.executer.FilterExecuter;
import org.carbondata.query.filter.executer.RowLevelFilterExecuterImpl;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;
import org.carbondata.query.util.QueryExecutorUtility;

public class RowLevelFilterResolverImpl extends ConditionalFilterResolverImpl {
	
	private ArrayList<DimColumnEvaluatorInfo> dimColEvaluatorInfoList;
	private ArrayList<MsrColumnEvalutorInfo> msrColEvalutorInfoList;
    protected Expression exp;
    protected boolean isExpressionResolve;
    protected boolean isIncludeFilter;
    public RowLevelFilterResolverImpl(Expression exp, boolean isExpressionResolve,
            boolean isIncludeFilter) {
    	super(exp, isExpressionResolve, isIncludeFilter);
    }

    @Override
    public void resolve(FilterEvaluatorInfo info) {
        DimColumnEvaluatorInfo dimColumnEvaluatorInfo = null;
        MsrColumnEvalutorInfo msrColumnEvalutorInfo = null;
        int index = 0;
        if (exp instanceof ConditionalExpression) {
            ConditionalExpression conditionalExpression = (ConditionalExpression) exp;
            List<ColumnExpression> columnList = conditionalExpression.getColumnList();
            for (ColumnExpression columnExpression : columnList) {
                if (columnExpression.isDimension()) {
                    dimColumnEvaluatorInfo = new DimColumnEvaluatorInfo();
                    dimColumnEvaluatorInfo.setRowIndex(index++);
                    dimColumnEvaluatorInfo.setSlices(info.getSlices());
					dimColumnEvaluatorInfo.setColumnIndex(info
							.getTableSegment()
							.getSegmentProperties()
							.getDimensionOrdinalToBlockMapping()
							.get(columnExpression.getDimension()
									.getOrdinal()));
                    dimColumnEvaluatorInfo.setCurrentSliceIndex(info.getCurrentSliceIndex());
                    dimColumnEvaluatorInfo.setDimension(columnExpression.getDimension());
                    dimColumnEvaluatorInfo.setComplexTypesWithBlockStartIndex(
                            info.getComplexTypesWithBlockStartIndex());
                    dimColumnEvaluatorInfo.setDimensions(info.getDimensions());
                    int newDimensionIndex = QueryExecutorUtility
                            .isNewDimension(info.getNewDimension(), columnExpression.getDim());
                    if (newDimensionIndex > -1) {
                        dimColumnEvaluatorInfo.setDimensionExistsInCurrentSilce(false);
                        dimColumnEvaluatorInfo.setRsSurrogates(
                                info.getNewDimensionSurrogates()[newDimensionIndex]);
                        dimColumnEvaluatorInfo.setDefaultValue(
                                info.getNewDimensionDefaultValue()[newDimensionIndex]
                                        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL) ?
                                        null :
                                        info.getNewDimensionDefaultValue()[newDimensionIndex]);
                    }

                    dimColEvaluatorInfoList.add(dimColumnEvaluatorInfo);
                } else {
                    msrColumnEvalutorInfo = new MsrColumnEvalutorInfo();
                    msrColumnEvalutorInfo.setRowIndex(index++);
                    msrColumnEvalutorInfo
                            .setAggregator(((Measure) columnExpression.getDim()).getAggName());
                    int measureIndex = QueryExecutorUtility.isNewMeasure(info.getNewMeasures(),
                            ((Measure) columnExpression.getDim()));
                    // if measure is found then index returned will be > 0 .
                    // else it will be -1 . here if the measure is a newly added
                    // measure then index will be >0.
                    if (measureIndex < 0) {

                        msrColumnEvalutorInfo.setType(
                                info.getSlices().get(info.getCurrentSliceIndex())
                                        .getDataCache(info.getFactTableName())
                                        .getType()[((Measure) columnExpression.getDim())
                                        .getOrdinal()]);
                    } else {
                        msrColumnEvalutorInfo.setMeasureExistsInCurrentSlice(false);
                        msrColumnEvalutorInfo
                                .setDefaultValue(info.getNewDefaultValues()[measureIndex]);
                    }
                    msrColEvalutorInfoList.add(msrColumnEvalutorInfo);
                }
            }
        }
    }
    
    @Override
	public FilterExecuter getFilterExecuterInstance() {
		// TODO Auto-generated method stub
		return new RowLevelFilterExecuterImpl(dimColEvaluatorInfoList,msrColEvalutorInfoList,exp);
	}
}
