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
package org.carbondata.query.filters;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.impl.btree.BTreeNode;
import org.carbondata.core.carbon.datastore.impl.btree.BtreeDataRefNodeFinder;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;

public class FilterExpressionProcessor implements FilterProcessor {

	/**
	 * Implementation will provide the resolved form of filters based on the
	 * filter expression tree which is been passed.
	 * 
	 * @param expressionTree
	 *            , filter expression tree
	 * @param info
	 *            ,certain metadata required for resolving filter.
	 * @return
	 */
	public FilterResolverIntf getFilterResolver(Expression expressionTree,
			FilterEvaluatorInfo info) {
		if (null != expressionTree && null != info) {
			FilterUtil.getFilterResolver(expressionTree, info);
			
		}

		return null;
	}

	/**
	 * This API will scan the Segment level all btrees and selects the required blocks
	 * inorder to push the same to executer.
	 */
	public List<DataRefNode> getFilterredBlocks(
			List<BTreeNode> listOfTree,
			FilterResolverIntf filterResolver, FilterEvaluatorInfo filterInfo) {
		// Need to get the current dimension tables
		List<DataRefNode> listOfDataBlocksToScan = new ArrayList<DataRefNode>();
		long[] startKey = new long[filterInfo.getTableSegment().getSegmentProperties().getDimensionKeyGenerator().getDimCount()];
		Map<Dimension, List<DimColumnFilterInfo>> dimensionFilter = filterInfo
				.getInfo().getDimensionFilter();
		for (Entry<Dimension, List<DimColumnFilterInfo>> entry : dimensionFilter
				.entrySet()) {
			List<DimColumnFilterInfo> values = entry.getValue();
			if (null == values) {
				continue;
			}
			boolean isExcludePresent = false;
			for (DimColumnFilterInfo info : values) {
				if (!info.isIncludeFilter()) {
					isExcludePresent = true;
				}
			}
			if (isExcludePresent) {
				continue;
			}
			for (DimColumnFilterInfo info : values) {
				if (startKey[entry.getKey().getOrdinal()] < info
						.getFilterList().get(0)) {
					startKey[entry.getKey().getOrdinal()] = info
							.getFilterList().get(0);
				}
			}
		}
		IndexKey serachKey = createIndexKeyFromResolvedFilterVal(startKey,
				filterInfo);
		for (BTreeNode btreeNode : listOfTree) {
			DataRefNodeFinder blockFinder = new BtreeDataRefNodeFinder(filterInfo.getTableSegment().getSegmentProperties().getDimensionColumnsValueSize());
			
			DataRefNode startBlock =blockFinder.findFirstDataBlock(btreeNode.getNextDataRefNode(), serachKey);
			DataRefNode endBlock = blockFinder.findLastDataBlock(btreeNode.getNextDataRefNode(), serachKey);
			
			
			while (startBlock != endBlock) {
				startBlock=startBlock.getNextDataRefNode();
				addBlockBasedOnMinMaxValue(filterResolver, listOfDataBlocksToScan,startBlock);
			}
			addBlockBasedOnMinMaxValue(filterResolver, listOfDataBlocksToScan,endBlock);
			
		}
		return listOfDataBlocksToScan;
	}

	/**
	 * Selects the blocks based on col max and min value.
	 * @param filterResolver
	 * @param listOfDataBlocksToScan
	 * @param dataBlock
	 */
	private void addBlockBasedOnMinMaxValue(FilterResolverIntf filterResolver,
			List<DataRefNode> listOfDataBlocksToScan, DataRefNode dataRefNode) {
		BitSet bitSet = filterResolver.getFilterExecuterInstance()
				.isScanRequired(dataRefNode.getColumnsMaxValue(),
						dataRefNode.getColumnsMinValue());
		if (!bitSet.isEmpty()) {
			listOfDataBlocksToScan.add(dataRefNode);

		}
	}

	private IndexKey createIndexKeyFromResolvedFilterVal(long[] startKey,
			FilterEvaluatorInfo filterInfo) {
		IndexKey indexKey = null;
		try {
			indexKey = new IndexKey();
			
			indexKey.setDictionaryKeys(filterInfo.getTableSegment()
					.getSegmentProperties().getDimensionKeyGenerator()
					.generateKey(startKey));

		} catch (KeyGenException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return indexKey;
	}

}
