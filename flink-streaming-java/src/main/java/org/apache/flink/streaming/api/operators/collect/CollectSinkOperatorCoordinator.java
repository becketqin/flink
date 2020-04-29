/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.CoordinationResponser;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * {@link OperatorCoordinator} for {@link CollectSinkOperator}.
 *
 * <p>This coordinator only forwards requests and responses from clients and sinks,
 * it does not store any state in itself.
 */
public class CollectSinkOperatorCoordinator implements OperatorCoordinator, CoordinationResponser {

	private static final Logger LOG = LoggerFactory.getLogger(CollectSinkFunction.class);

	private InetSocketAddress address;
	private Socket socket;

	@Override
	public void start() throws Exception {
	}

	@Override
	public void close() throws Exception {
		closeCurrentConnection();
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		Preconditions.checkArgument(
			event instanceof CollectSinkAddressEvent, "Operator event must be a CollectSinkAddressEvent");
		address = ((CollectSinkAddressEvent) event).getAddress();
		LOG.debug("Received sink socket server address: " + address);
	}

	@Override
	public CoordinationResponse handleCoordinationRequest(CoordinationRequest request) {
		Preconditions.checkArgument(
			request instanceof CollectCoordinationRequest,
			"Coordination request must be a CollectCoordinationRequest");

		CollectCoordinationRequest collectRequest = (CollectCoordinationRequest) request;

		if (address == null) {
			return new CollectCoordinationResponse(createEmptySerializedResult(collectRequest));
		}

		try {
			if (socket == null) {
				socket = new Socket(address.getAddress(), address.getPort());
				LOG.debug("Sink connection established");
			}

			// send version and token to sink server
			LOG.debug("Forwarding request to sink socket server");
			DataOutputViewStreamWrapper outStream = new DataOutputViewStreamWrapper(socket.getOutputStream());
			outStream.write(collectRequest.getBytes());

			// fetch back serialized results
			LOG.debug("Fetching serialized result from sink socket server");
			DataInputViewStreamWrapper inStream = new DataInputViewStreamWrapper(socket.getInputStream());
			int size = inStream.readInt();
			byte[] bytes = new byte[size];
			inStream.readFully(bytes);

			return new CollectCoordinationResponse(bytes);
		} catch (IOException e) {
			// request failed, close current connection and send back empty results
			closeCurrentConnection();
			return new CollectCoordinationResponse(createEmptySerializedResult(collectRequest));
		}
	}

	private byte[] createEmptySerializedResult(CollectCoordinationRequest request) {
		ByteArrayInputStream bais = new ByteArrayInputStream(request.getBytes());
		DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
		try {
			CollectCoordinationRequest.DeserializedRequest deserializedRequest =
				CollectCoordinationRequest.deserialize(wrapper);
			return CollectCoordinationResponse.serialize(
				deserializedRequest.getVersion(), deserializedRequest.getToken(), Collections.emptyList(), null);
		} catch (IOException e) {
			// impossible, as the data is read from an already presented byte array
			return new byte[0];
		}
	}

	private void closeCurrentConnection() {
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
				LOG.warn("Failed to close sink socket server connection", e);
			}
			socket = null;
		}
	}

	@Override
	public void subtaskFailed(int subtask) {
		// subtask failed, the socket server does not exist anymore
		address = null;
		socket = null;
	}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(address);
		return CompletableFuture.completedFuture(baos.toByteArray());
	}

	@Override
	public void checkpointComplete(long checkpointId) {
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
		ObjectInputStream ois = new ObjectInputStream(bais);
		address = (InetSocketAddress) ois.readObject();
	}

	/**
	 * Provider for {@link CollectSinkOperatorCoordinator}.
	 */
	public static class Provider implements OperatorCoordinator.Provider {

		private final OperatorID operatorId;

		public Provider(OperatorID operatorId) {
			this.operatorId = operatorId;
		}

		@Override
		public OperatorID getOperatorId() {
			return operatorId;
		}

		@Override
		public OperatorCoordinator create(Context context) {
			// we do not send operator event so we don't need a context
			return new CollectSinkOperatorCoordinator();
		}
	}
}
