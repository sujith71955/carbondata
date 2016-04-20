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

package org.carbondata.query.filters.measurefilter.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.carbonfilterinterface.RowIntf;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.datastorage.MemberStore;
import org.carbondata.query.expression.BinaryExpression;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.ExpressionResult;
import org.carbondata.query.expression.conditional.BinaryConditionalExpression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.ConditionalFilterResolverImpl;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filter.resolver.LogicalFilterResolverImpl;
import org.carbondata.query.filter.resolver.RestructureFilterResolverImpl;
import org.carbondata.query.filter.resolver.RowLevelFilterResolverImpl;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.DataTypeConverter;
import org.carbondata.query.util.QueryExecutorUtility;

//import org.carbondata.core.engine.util.CarbonEngineLogEvent;

public final class FilterUtil {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FilterUtil.class.getName());

    private FilterUtil() {

    }

    public static FilterResolverIntf getFilterResolver(Expression expressionTree,
    		FilterEvaluatorInfo info) {
    	FilterResolverIntf filterEvaluatorTree = createFilterResolverTree(expressionTree, info);
        traverseAndResolveTree(filterEvaluatorTree, info);
        return filterEvaluatorTree;
    }

    private static void traverseAndResolveTree(FilterResolverIntf filterEvaluatorTree,
            FilterEvaluatorInfo info) {
        if (null == filterEvaluatorTree) {
            return;
        }
        traverseAndResolveTree(filterEvaluatorTree.getLeft(), info);

        filterEvaluatorTree.resolve(info);

        traverseAndResolveTree(filterEvaluatorTree.getRight(), info);
    }

    private static FilterResolverIntf createFilterResolverTree(Expression expressionTree,
            FilterEvaluatorInfo info) {
        ExpressionType filterExpressionType = expressionTree.getFilterExpressionType();
        BinaryExpression currentExpression = null;
        switch (filterExpressionType) {
        case OR:
            currentExpression = (BinaryExpression) expressionTree;
            return new LogicalFilterResolverImpl(
                    createFilterResolverTree(currentExpression.getLeft(), info),
                    createFilterResolverTree(currentExpression.getRight(), info),filterExpressionType);
        case AND:
            currentExpression = (BinaryExpression) expressionTree;
            return new LogicalFilterResolverImpl(
                    createFilterResolverTree(currentExpression.getLeft(), info),
                    createFilterResolverTree(currentExpression.getRight(), info),filterExpressionType);
        case EQUALS:
        case IN:
            return getFilterResolverBasedOnExpressionType(ExpressionType.EQUALS, false, expressionTree, info,
                    expressionTree);

        case GREATERTHAN:
        case GREATERYHAN_EQUALTO:
        case LESSTHAN:
        case LESSTHAN_EQUALTO:
            return getFilterResolverBasedOnExpressionType(ExpressionType.EQUALS, true, expressionTree, info,
                    expressionTree);

        case NOT_EQUALS:
        case NOT_IN:
            return getFilterResolverBasedOnExpressionType(ExpressionType.NOT_EQUALS, false, expressionTree,
                    info, expressionTree);

        default:
            return getFilterResolverBasedOnExpressionType(ExpressionType.UNKNOWN, false, expressionTree, info,
                    expressionTree);
        }
    }

    private static FilterResolverIntf getFilterResolverBasedOnExpressionType(ExpressionType filterExpressionType,
            boolean isExpressionResolve, Expression expression, FilterEvaluatorInfo info,
            Expression expressionTree) {
        BinaryConditionalExpression currentCondExpression = null;
        ConditionalExpression condExpression = null;
        switch (filterExpressionType) {
        case EQUALS:
            currentCondExpression = (BinaryConditionalExpression) expression;
            if (currentCondExpression.isSingleDimension() &&
                    currentCondExpression.getColumnList().get(0).getDim().getDataType()
                            != Type.ARRAY &&
                    currentCondExpression.getColumnList().get(0).getDim().getDataType()
                            != Type.STRUCT) {
               //getting new dim index.
                int newDimensionIndex = QueryExecutorUtility.isNewDimension(info.getNewDimension(),
                        currentCondExpression.getColumnList().get(0).getDim());
                if (newDimensionIndex == -1) {
                    if (currentCondExpression.getColumnList().get(0).getDim()
                            .isNoDictionaryDim()) {
                        if (checkIfExpressionContainsColumn(currentCondExpression.getLeft())
                                || checkIfExpressionContainsColumn(
                                currentCondExpression.getRight())) {
                            return new RowLevelFilterResolverImpl(expression, isExpressionResolve,
                                    true);
                        }

                        if (expressionTree.getFilterExpressionType() == ExpressionType.GREATERTHAN
                                || expressionTree.getFilterExpressionType()
                                == ExpressionType.LESSTHAN
                                || expressionTree.getFilterExpressionType()
                                == ExpressionType.GREATERYHAN_EQUALTO
                                || expressionTree.getFilterExpressionType()
                                == ExpressionType.LESSTHAN_EQUALTO) {
                            return new RowLevelFilterResolverImpl(expression, isExpressionResolve,
                                    true);
                        }
                        return new ConditionalFilterResolverImpl(expression, isExpressionResolve,
                                true);
                    }else {
                        return new ConditionalFilterResolverImpl(expression, isExpressionResolve,
                                true);
                    }
                } else {
                    return new RestructureFilterResolverImpl(expression,
                            info.getNewDimensionDefaultValue()[newDimensionIndex],
                            info.getNewDimensionSurrogates()[newDimensionIndex],
                            isExpressionResolve,true);
                }
            }
        case NOT_EQUALS:

            currentCondExpression = (BinaryConditionalExpression) expression;
            if (currentCondExpression.isSingleDimension() &&
                    currentCondExpression.getColumnList().get(0).getDim().getDataType()
                            != Type.ARRAY &&
                    currentCondExpression.getColumnList().get(0).getDim().getDataType()
                            != Type.STRUCT) {
                int newDimensionIndex = QueryExecutorUtility.isNewDimension(info.getNewDimension(),
                        currentCondExpression.getColumnList().get(0).getDim());
                if (newDimensionIndex == -1) {
                    if (currentCondExpression.getColumnList().get(0).getDim()
                            .isNoDictionaryDim()) {
                        if (checkIfExpressionContainsColumn(currentCondExpression.getLeft())
                                || checkIfExpressionContainsColumn(
                                currentCondExpression.getRight())) {
                            return new RowLevelFilterResolverImpl(expression, isExpressionResolve,
                                    false);
                        }
                        if (expressionTree.getFilterExpressionType() == ExpressionType.GREATERTHAN
                                || expressionTree.getFilterExpressionType()
                                == ExpressionType.LESSTHAN
                                || expressionTree.getFilterExpressionType()
                                == ExpressionType.GREATERYHAN_EQUALTO
                                || expressionTree.getFilterExpressionType()
                                == ExpressionType.LESSTHAN_EQUALTO) {
                            return new RowLevelFilterResolverImpl(expression, isExpressionResolve,
                                    false);
                        }
                        return new ConditionalFilterResolverImpl(expression, isExpressionResolve,
                                false);
                    }
                    return new ConditionalFilterResolverImpl(expression, isExpressionResolve,
                            false);
                } else {
                    return new RestructureFilterResolverImpl(expression,
                            info.getNewDimensionDefaultValue()[newDimensionIndex],
                            info.getNewDimensionSurrogates()[newDimensionIndex],
                            isExpressionResolve,false);
                }
            }
        default:
            condExpression = (ConditionalExpression) expression;
            if (condExpression.isSingleDimension() &&
                    condExpression.getColumnList().get(0).getDim().getDataType() != Type.ARRAY &&
                    condExpression.getColumnList().get(0).getDim().getDataType() != Type.STRUCT) {
                condExpression = (ConditionalExpression) expression;
                if (condExpression.isSingleDimension()) {
                    int newDimensionIndex = QueryExecutorUtility
                            .isNewDimension(info.getNewDimension(),
                                    condExpression.getColumnList().get(0).getDim());
                    if (newDimensionIndex == -1) {
                        if (condExpression.getColumnList().get(0).getDim().isNoDictionaryDim()) {
                            if (checkIfExpressionContainsColumn(currentCondExpression.getLeft())
                                    || checkIfExpressionContainsColumn(
                                    currentCondExpression.getRight())) {
                                return new RowLevelFilterResolverImpl(expression, isExpressionResolve,
                                        false);
                            } else if (expressionTree.getFilterExpressionType()
                                    == ExpressionType.UNKNOWN) {
                                return new RowLevelFilterResolverImpl(expression, false, false);
                            }
                            return new ConditionalFilterResolverImpl(expression, true, true);
                        }
                  
                            return new ConditionalFilterResolverImpl(expression, true, true);
                        
                    } else {
                        return new RestructureFilterResolverImpl(expression,
                                info.getNewDimensionDefaultValue()[newDimensionIndex],
                                info.getNewDimensionSurrogates()[newDimensionIndex], true,true);
                    }
                } else {
                    return new RowLevelFilterResolverImpl(expression, false, false);
                }
            } else {
                return new RowLevelFilterResolverImpl(expression, false, false);
            }
        }
    }

    /**
     * This method will check if a given expression contains a column expression recursively.
     *
     * @param right
     * @return
     */
    public static boolean checkIfExpressionContainsColumn(Expression expression) {
        if (expression instanceof ColumnExpression) {
            return true;
        }
        for (Expression child : expression.getChildren()) {
            if (checkIfExpressionContainsColumn(child)) {
                return true;
            }
        }

        return false;
    }

    private static byte[] getMaskedKey(int[] ranges, byte[] key) {
        byte[] maskkey = new byte[ranges.length];

        for (int i = 0; i < maskkey.length; i++) {
            //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
            maskkey[i] = key[ranges[i]];
        }
        //CHECKSTYLE:ON
        return maskkey;
    }

    /**
     * This method will return the ranges for the masked Bytes based on the key
     * Generator.
     *
     * @param queryDimensions
     * @param generator
     * @return
     */
    private static int[] getRangesForMaskedByte(int queryDimensionsOrdinal,
            KeyGenerator generator) {
        Set<Integer> integers = new TreeSet<Integer>();
        int[] range = generator.getKeyByteOffsets(queryDimensionsOrdinal);
        for (int j = range[0]; j <= range[1]; j++) {
            integers.add(j);
        }

        int[] byteIndexs = new int[integers.size()];
        int j = 0;
        for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
            Integer integer = iterator.next();
            byteIndexs[j++] = integer.intValue();
        }
        return byteIndexs;
    }

    private static List<byte[]> getNoDictionaryValKeyMemberForFilter(FilterEvaluatorInfo info,
            ColumnExpression columnExpression, List<String> evaluateResultListFinal,
            boolean isIncludeFilter) {
        List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
        for (String result : evaluateResultListFinal) {
            filterValuesList.add(result.getBytes());
        }
        return filterValuesList;
    }

	public static List<byte[]> getFilterValues(FilterEvaluatorInfo info,
			ColumnExpression columnExpression, List<String> evaluateResultList,
			boolean isIncludeFilter)

	{
		List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
		int[] keys = new int[info.getKeyGenerator().getDimCount()];
		Arrays.fill(keys, 0);
		int[] rangesForMaskedByte = getRangesForMaskedByte(
				(columnExpression.getDimension().getOrdinal()), info
						.getTableSegment().getSegmentProperties()
						.getDimensionKeyGenerator());
		List<Integer> surrogates = new ArrayList<Integer>(20);
		for (String resultVal : evaluateResultList) {
			// Reading the dictionary value from cache.
			Integer dictionaryVal = getDictionaryValue(resultVal, info,
					columnExpression.getDimension());
			if (null != dictionaryVal) {
				surrogates.add(dictionaryVal);
			}
		}
		Collections.sort(surrogates);
		// CHECKSTYLE:OFF Approval No:Approval-V1R2C10_007
		for (Integer surrogate : surrogates) {
			try {
				keys[columnExpression.getDimension().getOrdinal()] = surrogate;
				filterValuesList.add(getMaskedKey(rangesForMaskedByte, info
						.getTableSegment().getSegmentProperties()
						.getDimensionKeyGenerator().generateKey(keys)));
			} catch (KeyGenException e) {
				LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
						e.getMessage());
			}
		}
		// CHECKSTYLE:ON
		DimColumnFilterInfo columnFilterInfo = null;
		if (surrogates.size() > 0) {
			columnFilterInfo = new DimColumnFilterInfo();
			columnFilterInfo.setIncludeFilter(isIncludeFilter);
			columnFilterInfo.setFilterList(surrogates);
			info.getInfo().addDimensionFilter(columnExpression.getDim(),
					columnFilterInfo);
		}
		return filterValuesList;
	}

	/**
	 * This API will get the Dictionary value for the respective filter member
	 * string.
	 * 
	 * @param value
	 *            filter value
	 * @param filterEval
	 * @param dim
	 *            , column expression dimension type.
	 * @return the dictionary value.
	 */
	private static Integer getDictionaryValue(String value,
			FilterEvaluatorInfo filterEval, CarbonDimension dim) {
		DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
				filterEval.getAbsoluteTableIdentifier()
						.getCarbonTableIdentifier(), String.valueOf(dim
						.getColumnId()));
		CacheProvider cacheProvider = CacheProvider.getInstance();
		Cache forwardDictionaryCache = cacheProvider.createCache(
				CacheType.FORWARD_DICTIONARY, filterEval
						.getAbsoluteTableIdentifier().getStorePath());
		// get the forward dictionary object
		Dictionary forwardDictionary = (Dictionary) forwardDictionaryCache
				.get(dictionaryColumnUniqueIdentifier);
		if (null != forwardDictionary) {
			forwardDictionary.getSurrogateKey(value);
		}
		return null;
	}

    public static byte[][] getFilterListForAllMembers(FilterEvaluatorInfo info,
            Expression expression, ColumnExpression columnExpression, boolean isIncludeFilter) {
        List<byte[]> filterValuesList = null;
        List<String> evaluateResultListFinal = new ArrayList<String>(20);
        MemberStore memberStore = null;
        for (int i = 0; i <= info.getCurrentSliceIndex(); i++) {
            memberStore = info.getSlices().get(i).getMemberCache(
                    columnExpression.getDim().getTableName() + '_' + columnExpression.getDim()
                            .getColName() + '_' + columnExpression.getDim().getDimName() + '_'
                            + columnExpression.getDim().getHierName());
            if (null == memberStore) {
                continue;
            }
            Member[][] allMembers = memberStore.getAllMembers();

            //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_007
            for (int j = 0; j < allMembers.length; j++) {
                for (int k = 0; k < allMembers[j].length; k++) {
                    try {
                        RowIntf row = new RowImpl();
                        String string = allMembers[j][k].toString();
                        if (string.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                            string = null;
                        }
                        row.setValues(new Object[] { DataTypeConverter
                                .getDataBasedOnDataType(string,
                                        columnExpression.getDim().getDataType()) });
                        Boolean rslt = expression.evaluate(row).getBoolean();
                        if (null != rslt && !(rslt ^ isIncludeFilter)) {
                            if (null == string) {
                                evaluateResultListFinal
                                        .add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
                            } else {
                                evaluateResultListFinal.add(string);
                            }
                        }
                    } catch (FilterUnsupportedException e) {
                        LOGGER.audit(e.getMessage());
                    }
                }
            }
            //CHECKSTYLE:ON
        }
        filterValuesList =
                getFilterValues(info, columnExpression, evaluateResultListFinal, isIncludeFilter);
        return filterValuesList.toArray(new byte[filterValuesList.size()][]);
    }
    public static byte[][] getFilterListForAllValues(FilterEvaluatorInfo info,
            Expression expression, ColumnExpression columnExpression, boolean isIncludeFilter) {
    	DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
    			info.getAbsoluteTableIdentifier().getCarbonTableIdentifier(),
				String.valueOf(columnExpression.getDimension().getColumnId()));
		CacheProvider cacheProvider = CacheProvider.getInstance();
		Cache forwardDictionaryCache = cacheProvider.createCache(
				CacheType.FORWARD_DICTIONARY, info.getAbsoluteTableIdentifier()
						.getStorePath());
		// get the forward dictionary object
		Dictionary forwardDictionary = (Dictionary) forwardDictionaryCache
				.get(dictionaryColumnUniqueIdentifier);
		 List<byte[]> filterValuesList = null;
	        List<String> evaluateResultListFinal = new ArrayList<String>(20);
		DictionaryChunksWrapper dictionaryWrapper=forwardDictionary.getDictionaryChunks();
		while(dictionaryWrapper.hasNext())
		{
			byte[] columnVal= dictionaryWrapper.next();
			try {
                RowIntf row = new RowImpl();
                String stringValue =new String(columnVal);
                if (stringValue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                	stringValue = null;
                }
                row.setValues(new Object[] { DataTypeConverter
                        .getDataBasedOnDataType(stringValue,
                                columnExpression.getDim().getDataType()) });
                Boolean rslt = expression.evaluate(row).getBoolean();
                if (null != rslt && !(rslt ^ isIncludeFilter)) {
                    if (null == stringValue) {
                        evaluateResultListFinal
                                .add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
                    } else {
                        evaluateResultListFinal.add(stringValue);
                    }
                }
            } catch (FilterUnsupportedException e) {
                LOGGER.audit(e.getMessage());
            }
		}
		filterValuesList =
                getFilterValues(info, columnExpression, evaluateResultListFinal, isIncludeFilter);
        return filterValuesList.toArray(new byte[filterValuesList.size()][]);
    }
    public static byte[][] getFilterList(FilterEvaluatorInfo info, Expression expression,
            ColumnExpression columnExpression, boolean isIncludeFilter) {
        List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
        List<String> evaluateResultListFinal = new ArrayList<String>(20);
        try {
            List<ExpressionResult> evaluateResultList = expression.evaluate(null).getList();
            for (ExpressionResult result : evaluateResultList) {
                if (result.getString() == null) {
                    evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
                    continue;
                }
                evaluateResultListFinal.add(result.getString());
            }
            if (null != columnExpression.getDim() && columnExpression.getDim()
                    .isNoDictionaryDim()) {
                filterValuesList = getNoDictionaryValKeyMemberForFilter(info, columnExpression,
                        evaluateResultListFinal, isIncludeFilter);
            } else {
                filterValuesList = getFilterValues(info, columnExpression, evaluateResultListFinal,
                        isIncludeFilter);
            }
        } catch (FilterUnsupportedException e) {
            LOGGER.audit(e.getMessage());
        }
        return filterValuesList.toArray(new byte[filterValuesList.size()][]);
    }

    public static byte[][] getFilterListForRS(Expression expression,
            ColumnExpression columnExpression, String defaultValues, int defaultSurrogate) {
        List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
        List<String> evaluateResultListFinal = new ArrayList<String>(20);
        KeyGenerator keyGenerator =
                KeyGeneratorFactory.getKeyGenerator(new int[] { defaultSurrogate });
        try {
            List<ExpressionResult> evaluateResultList = expression.evaluate(null).getList();
            for (ExpressionResult result : evaluateResultList) {
                if (result.getString() == null) {
                    evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
                    continue;
                }
                evaluateResultListFinal.add(result.getString());
            }

            for (int i = 0; i < evaluateResultListFinal.size(); i++) {
                if (evaluateResultListFinal.get(i).equals(defaultValues)) {
                    filterValuesList.add(keyGenerator.generateKey(new int[] { defaultSurrogate }));
                    break;
                }
            }
        } catch (FilterUnsupportedException e) {
            LOGGER.audit(e.getMessage());
        } catch (KeyGenException e) {
            LOGGER.audit(e.getMessage());
        }
        return filterValuesList.toArray(new byte[filterValuesList.size()][]);
    }

    public static byte[][] getFilterListForAllMembersRS(Expression expression,
            ColumnExpression columnExpression, String defaultValues, int defaultSurrogate,
            boolean isIncludeFilter) {
        List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
        List<String> evaluateResultListFinal = new ArrayList<String>(20);
        KeyGenerator keyGenerator =
                KeyGeneratorFactory.getKeyGenerator(new int[] { defaultSurrogate });
        try {
            RowIntf row = new RowImpl();
            if (defaultValues.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                defaultValues = null;
            }
            row.setValues(new Object[] { DataTypeConverter.getDataBasedOnDataType(defaultValues,
                    columnExpression.getDim().getDataType()) });
            Boolean rslt = expression.evaluate(row).getBoolean();
            if (null != rslt && !(rslt ^ isIncludeFilter)) {
                if (null == defaultValues) {
                    evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
                } else {
                    evaluateResultListFinal.add(defaultValues);
                }
            }
        } catch (FilterUnsupportedException e) {
            LOGGER.audit(e.getMessage());
        }

        if (null == defaultValues) {
            defaultValues = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
        }
        try {
            for (int i = 0; i < evaluateResultListFinal.size(); i++) {
                if (evaluateResultListFinal.get(i).equals(defaultValues)) {
                    filterValuesList.add(keyGenerator.generateKey(new int[] { defaultSurrogate }));
                    break;
                }
            }
        } catch (KeyGenException e) {
            LOGGER.audit(e.getMessage());
        }
        return filterValuesList.toArray(new byte[filterValuesList.size()][]);
    }

}