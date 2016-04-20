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

import org.carbondata.query.carbonfilterinterface.ExpressionType;
import org.carbondata.query.filter.executer.AndFilterExecuterImpl;
import org.carbondata.query.filter.executer.FilterExecuter;
import org.carbondata.query.filter.executer.OrFilterExecuterImpl;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;

public class LogicalFilterResolverImpl implements FilterResolverIntf {
	protected FilterResolverIntf leftEvalutor;

	protected FilterResolverIntf rightEvalutor;

	private ExpressionType filterExpressionType;

	public LogicalFilterResolverImpl(FilterResolverIntf leftEvalutor,
			FilterResolverIntf rightEvalutor,ExpressionType filterExpressionType) {
		this.leftEvalutor = leftEvalutor;
		this.rightEvalutor = rightEvalutor;
		this.filterExpressionType=filterExpressionType;
	}

	@Override
	public void resolve(FilterEvaluatorInfo info) {

	}

	public FilterResolverIntf getLeft() {
		return leftEvalutor;
	}

	public FilterResolverIntf getRight() {
		return rightEvalutor;
	}

	@Override
	public FilterExecuter getFilterExecuterInstance() {
		// TODO Auto-generated method stub
		switch (filterExpressionType) {
		case OR:
			new OrFilterExecuterImpl(leftEvalutor, rightEvalutor);
		case AND:
			return new AndFilterExecuterImpl(leftEvalutor, rightEvalutor);

		default:
			return null;
		}
	}

}
