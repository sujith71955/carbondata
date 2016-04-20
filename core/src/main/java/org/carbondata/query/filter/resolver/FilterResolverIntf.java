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

import org.carbondata.query.filter.executer.FilterExecuter;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;

public interface FilterResolverIntf {

	/**
	 * This API will resolve the filter expression and generates the
	 * dictionaries for executing/evaluating the filter expressions in the
	 * executer layer.
	 * 
	 * @param info
	 */
	void resolve(FilterEvaluatorInfo info);

	/**
	 * This API will provide the left column filter expression
	 * 
	 * @return FilterResolverIntf
	 */
	FilterResolverIntf getLeft();

	/**
	 * API will provide the right column filter expression
	 * 
	 * @return FilterResolverIntf
	 */
	FilterResolverIntf getRight();

	/**
	 * This API will get the filter executer instance which is required to
	 * evaluate/execute the resolved filter expressions in the executer layer.
	 * 
	 * @return FilterExecuter instance.
	 */
	FilterExecuter getFilterExecuterInstance();

}
