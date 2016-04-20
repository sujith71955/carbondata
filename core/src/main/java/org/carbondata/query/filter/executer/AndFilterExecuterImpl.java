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

import java.util.BitSet;

import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.evaluators.FilterProcessorPlaceHolder;
import org.carbondata.query.filter.resolver.FilterResolverIntf;

public class AndFilterExecuterImpl implements FilterExecuter {

	private FilterResolverIntf leftEvalutor;
	private FilterResolverIntf rightEvalutor;

	public AndFilterExecuterImpl(FilterResolverIntf leftEvalutor,
			FilterResolverIntf rightEvalutor) {
		this.leftEvalutor = leftEvalutor;
		this.rightEvalutor = rightEvalutor;
	}

	@Override
	public BitSet applyFilter(BlockDataHolder blockDataHolder,
			FilterProcessorPlaceHolder placeHolder, int[] noDictionaryColIndexes) {
		BitSet leftFilters = leftEvalutor.getFilterExecuterInstance()
				.applyFilter(blockDataHolder, placeHolder,
						noDictionaryColIndexes);
		if (leftFilters.isEmpty()) {
			return leftFilters;
		}
		BitSet rightFilter = rightEvalutor.getFilterExecuterInstance()
				.applyFilter(blockDataHolder, placeHolder,
						noDictionaryColIndexes);
		if (rightFilter.isEmpty()) {
			return rightFilter;
		}
		leftFilters.and(rightFilter);
		return leftFilters;
	}

	@Override
	public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
		BitSet leftFilters = leftEvalutor.getFilterExecuterInstance()
				.isScanRequired(blockMaxValue, blockMinValue);
		if (leftFilters.isEmpty()) {
			return leftFilters;
		}
		BitSet rightFilter = rightEvalutor.getFilterExecuterInstance()
				.isScanRequired(blockMaxValue, blockMinValue);
		if (rightFilter.isEmpty()) {
			return rightFilter;
		}
		leftFilters.and(rightFilter);
		return leftFilters;
	}

}
