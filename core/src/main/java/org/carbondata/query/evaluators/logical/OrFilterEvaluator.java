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

package org.carbondata.query.evaluators.logical;

import java.util.BitSet;

import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.evaluators.FilterEvaluator;
import org.carbondata.query.evaluators.FilterProcessorPlaceHolder;

public class OrFilterEvaluator extends AbstractLogicalFilterEvaluator {
    public OrFilterEvaluator(FilterEvaluator leftEvalutor, FilterEvaluator rightEvalutor) {
        super(leftEvalutor, rightEvalutor);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder blockDataHolder,
            FilterProcessorPlaceHolder placeHolder,int [] directSurrogates) {
        BitSet leftFilters = leftEvalutor.applyFilter(blockDataHolder, placeHolder,directSurrogates);
        BitSet rightFilters = rightEvalutor.applyFilter(blockDataHolder, placeHolder,directSurrogates);
        leftFilters.or(rightFilters);
        return leftFilters;
    }

    @Override
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
        BitSet leftFilters = leftEvalutor.isScanRequired(blockMaxValue, blockMinValue);
        BitSet rightFilters = rightEvalutor.isScanRequired(blockMaxValue, blockMinValue);
        leftFilters.or(rightFilters);
        return leftFilters;
    }

}
