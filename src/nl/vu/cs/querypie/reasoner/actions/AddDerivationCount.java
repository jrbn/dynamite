package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

public class AddDerivationCount extends Action {

	private final Tuple previousTuple = TupleFactory.newTuple();
	private boolean first = true;
	private int currentCount = 0;
	private final TInt outputCount = new TInt();

	private final SimpleData[] outputTuple = new SimpleData[4];

	@Override
	public void startProcess(ActionContext context) throws Exception {
		outputTuple[3] = outputCount;
		first = true;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (first) {
			first = false;
			currentCount = 1;
			tuple.copyTo(previousTuple);
		} else if (!previousTuple.equals(tuple)) {
			outputCount.setValue(currentCount);
			actionOutput.output(previousTuple);
			currentCount = 1;
			tuple.copyTo(previousTuple);
		} else {
			currentCount++;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (!first) {
			outputCount.setValue(currentCount);
			actionOutput.output(previousTuple);
		}
	}
}
