package example;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.giraph.graph.BasicComputation;



public class PageRank
		extends
		BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

	/* Maximum number of iterations */
	public final static long MAX_STEPS = 50;

	@Override
	public void compute(
			Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {

		if (getSuperstep() >= 1) {
			double sum = 0;
			/* Collect PageRank from incoming neighbors */
			for (DoubleWritable msg : messages) {
				sum += msg.get();
			}

			/* Update PageRank */
			DoubleWritable vertexValue = new DoubleWritable(
					(0.15d / getTotalNumVertices()) + 0.85d * sum);
			vertex.setValue(vertexValue);

			//getValue().set((0.15d / getTotalNumVertices()) + 0.85d * sum);
		}

		if (getSuperstep() < MAX_STEPS) {
			/* Send updated PageRank to outgoing neighbors */
			sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue()
					.get() / vertex.getNumEdges()));
		} else {
			/* Stop */
			vertex.voteToHalt();
		}

	}

}
