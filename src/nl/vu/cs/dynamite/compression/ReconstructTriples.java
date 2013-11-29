package nl.vu.cs.dynamite.compression;

import java.io.DataOutputStream;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconstructTriples extends Action {

	protected static Logger log = LoggerFactory
			.getLogger(ReconstructTriples.class);

	long previousTripleId;
	long tripleId;
	long urlId;
	int position;
	TLong[] outputTriple = { new TLong(), new TLong(), new TLong() };
	Tuple outputTuple = TupleFactory.newTuple(outputTriple);

	DataOutputStream writer;

	int positions;
	// int checkSum;

	long countTriples;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		previousTripleId = Long.MIN_VALUE;
		// checkSum =
		positions = 0;
		countTriples = 0;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		tripleId = ((TLong) inputTuple.get(0)).getValue();
		urlId = ((TLong) inputTuple.get(1)).getValue();
		position = ((TByte) inputTuple.get(2)).getValue();

		if (previousTripleId != tripleId) {
			// checkSum =
			positions = 0;
			previousTripleId = tripleId;
		}

		outputTriple[position - 1].setValue(urlId);
		// checkSum += position - 1;
		positions++;

		if (positions == 3) {
			// if (checkSum != 3) {
			// log.error("Node=" + context.getMyNodeId() + " Checksum="
			// + checkSum + " id=" + tripleId);
			// } else {
			countTriples++;
			output.output(outputTuple);
			// }
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		context.incrCounter("output triples", countTriples);
	}
}
