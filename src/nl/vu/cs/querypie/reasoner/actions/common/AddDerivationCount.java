package nl.vu.cs.querypie.reasoner.actions.common;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class AddDerivationCount extends Action {
	public static void addToChain(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(AddDerivationCount.class);
		actions.add(c);
	}

	private boolean first;
	private int currentCount;

	private SimpleData[] outputTuple;
	private Tuple previousTuple;

	private int minStep;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		previousTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong(), new TInt());
		outputTuple = new SimpleData[5];
		for (int i = 0; i < 3; ++i) {
			outputTuple[i] = previousTuple.get(i);
		}
		outputTuple[3] = new TInt();
		outputTuple[4] = new TInt();
		first = true;
		minStep = Integer.MAX_VALUE;
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		if (!previousTuple.equals(tuple)) {
			if (first) {
				first = false;
			} else {
				generateOutput(actionOutput);
			}
			prepareForNewTuple(tuple);
		} else {
			minStep = Math.min(minStep, ((TInt) tuple.get(3)).getValue());
			currentCount++;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		if (!first) {
			generateOutput(actionOutput);
		}
	}

	private void prepareForNewTuple(Tuple tuple) {
		tuple.copyTo(previousTuple);
		currentCount = 1;
		minStep = ((TInt) tuple.get(3)).getValue();
	}

	private void generateOutput(ActionOutput actionOutput) throws Exception {
		((TInt) outputTuple[3]).setValue(minStep);
		((TInt) outputTuple[4]).setValue(currentCount);
		actionOutput.output(outputTuple);
	}
}
