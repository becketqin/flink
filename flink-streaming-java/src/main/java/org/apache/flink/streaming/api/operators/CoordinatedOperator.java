/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;

/**
 * An interface for an operator that has a {@link OperatorCoordinator}.
 */
public interface CoordinatedOperator extends OperatorEventHandler {

	/**
	 * Get the operator coordinator provider for this operator.
	 *
	 * @return the provider of the {@link CoordinatedOperator} for this operator.
	 */
	OperatorCoordinator.Provider getCoordinatorProvider();

	/**
	 * Sets the {@link OperatorEventGateway} for sending events to the operator coordinator.
	 * This method will be invoked before {@link StreamOperator#open()} is invoked.
	 *
	 * @param operatorEventGateway the {@link OperatorEventGateway} for sending {@link OperatorEvent}
	 *                             to the {@link OperatorCoordinator}.
	 */
	void setOperatorEventGateway(OperatorEventGateway operatorEventGateway);
}
