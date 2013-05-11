package nl.vu.cs.dynamite.index;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PermuteTriples extends Action {

	protected static final Logger log = LoggerFactory
			.getLogger(PermuteTriples.class);

	public static final int B_COUNT = 0;

	private boolean count;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_COUNT, "count", false, false);
	}

	private static final TInt one = new TInt(1);

	private TLong inputTriple0, inputTriple1, inputTriple2;
	private TInt step;
	private TInt c;
	private final Tuple outputTuple = TupleFactory.newTuple();

	@Override
	public void startProcess(ActionContext context) throws Exception {
		count = getParamBoolean(B_COUNT);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		inputTriple0 = (TLong) inputTuple.get(0);
		inputTriple1 = (TLong) inputTuple.get(1);
		inputTriple2 = (TLong) inputTuple.get(2);
		step = (TInt) inputTuple.get(3);

		if (!count) {

			outputTuple.set(Partitions.partition_ids[0], inputTriple0,
					inputTriple1, inputTriple2, step);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[1], inputTriple0,
					inputTriple2, inputTriple1, step);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[2], inputTriple1,
					inputTriple2, inputTriple0, step);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[3], inputTriple1,
					inputTriple0, inputTriple2, step);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[4], inputTriple2,
					inputTriple1, inputTriple0, step);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[5], inputTriple2,
					inputTriple0, inputTriple1, step);
			output.output(outputTuple);

		} else {

			if (inputTuple.getNElements() == 5) {
				c = (TInt) inputTuple.get(4);
			} else {
				c = one;
			}

			outputTuple.set(Partitions.partition_ids[0], inputTriple0,
					inputTriple1, inputTriple2, step, c);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[1], inputTriple0,
					inputTriple2, inputTriple1, step, c);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[2], inputTriple1,
					inputTriple2, inputTriple0, step, c);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[3], inputTriple1,
					inputTriple0, inputTriple2, step, c);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[4], inputTriple2,
					inputTriple1, inputTriple0, step, c);
			output.output(outputTuple);

			outputTuple.set(Partitions.partition_ids[5], inputTriple2,
					inputTriple0, inputTriple1, step, c);
			output.output(outputTuple);

		}
	}
}
