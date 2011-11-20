/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.streaming;

import java.io.IOException;
import java.util.Iterator;
<<<<<<< HEAD
import java.util.concurrent.ConcurrentHashMap;
=======
>>>>>>> streaming_test

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobGraph;
<<<<<<< HEAD
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.streaming.latency.LatencyOptimizerThread;
=======
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.template.AbstractInvokable;
>>>>>>> streaming_test

public class StreamingJobManagerPlugin implements JobManagerPlugin, JobStatusListener {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamingJobManagerPlugin.class);

	private ConcurrentHashMap<JobID, LatencyOptimizerThread> latencyOptimizerThreads = new ConcurrentHashMap<JobID, LatencyOptimizerThread>();

	StreamingJobManagerPlugin(final Configuration pluginConfiguration) {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobGraph rewriteJobGraph(final JobGraph jobGraph) {

		// Rewrite input vertices
		//final Iterator<AbstractJobInputVertex> inputIt = jobGraph.getInputVertices();

		final Iterator<JobTaskVertex> taskIt = jobGraph.getTaskVertices();
		while (taskIt.hasNext()) {

			final JobTaskVertex taskVertex = taskIt.next();

			final Class<? extends AbstractInvokable> originalClass = taskVertex.getInvokableClass();

			taskVertex.setTaskClass(TaskWrapper.class);
			taskVertex.getConfiguration().setString(TaskWrapper.WRAPPED_CLASS_KEY, originalClass.getName());
		}

		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionGraph rewriteExecutionGraph(final ExecutionGraph executionGraph) {
		JobID jobId = executionGraph.getJobID();
		LatencyOptimizerThread optimizerThread = new LatencyOptimizerThread(executionGraph);
		latencyOptimizerThreads.put(jobId, optimizerThread);
		optimizerThread.start();
		return executionGraph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		shutdownLatencyOptimizerThreads();
	}

	private void shutdownLatencyOptimizerThreads() {
		Iterator<LatencyOptimizerThread> iter = latencyOptimizerThreads.values().iterator();
		while (iter.hasNext()) {
			LatencyOptimizerThread thread = iter.next();
			thread.interrupt();

			// also removes jobID mappings from underlying map
			iter.remove();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendData(final IOReadableWritable data) throws IOException {

		if (!(data instanceof AbstractStreamingData)) {
			LOG.error("Received unexpected data of type " + data);
			return;
		}

		AbstractStreamingData streamingData = (AbstractStreamingData) data;
		LatencyOptimizerThread optimizerThread = latencyOptimizerThreads.get(streamingData.getJobID());
		optimizerThread.handOffStreamingData(streamingData);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {

		if (!(data instanceof AbstractStreamingData)) {
			LOG.error("Received unexpected data of type " + data);
			return null;
		}

		return null;
	}

	@Override
	public void jobStatusHasChanged(ExecutionGraph executionGraph,
			InternalJobStatus newJobStatus,
			String optionalMessage) {

		if (newJobStatus == InternalJobStatus.FAILED
			|| newJobStatus == InternalJobStatus.CANCELED
			|| newJobStatus == InternalJobStatus.FINISHED) {

			LatencyOptimizerThread optimizerThread = latencyOptimizerThreads.remove(executionGraph.getJobID());
			if (optimizerThread != null) {
				optimizerThread.interrupt();
			}
		}
	}
}
