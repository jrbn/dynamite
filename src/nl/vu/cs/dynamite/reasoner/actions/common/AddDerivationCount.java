package nl.vu.cs.dynamite.reasoner.actions.common;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddDerivationCount extends Action {

	public static final int I_MIN_STEP = 0;

	protected static final Logger log = LoggerFactory
			.getLogger(AddDerivationCount.class);

	public static void addToChain(ActionSequence actions, int currentStep)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(AddDerivationCount.class);
		c.setParamInt(I_MIN_STEP, currentStep);
		actions.add(c);
	}

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_MIN_STEP, "step", null, true);
	}

	private int currentCount;

	private SimpleData[] outputTuple;
	private Tuple previousTuple;
	private TLong tl1, tl2, tl3;
	private TInt step, count;

	private int minStep;
	private int stepToCount;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		tl1 = new TLong();
		tl2 = new TLong();
		tl3 = new TLong();
		step = new TInt();
		count = new TInt();
		previousTuple = TupleFactory.newTuple(tl1, tl2, tl3);
		outputTuple = new SimpleData[5];
		for (int i = 0; i < 3; ++i) {
			outputTuple[i] = previousTuple.get(i);
		}
		outputTuple[3] = step;
		outputTuple[4] = count;
		currentCount = 0;
		minStep = Integer.MAX_VALUE;
		stepToCount = getParamInt(I_MIN_STEP);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (!sameTriple(tuple)) {
			if (currentCount > 0) {
				generateOutput(actionOutput);
			}
			prepareForNewTuple(tuple);
		} else {
			int step = ((TInt) tuple.get(3)).getValue();
			minStep = Math.min(minStep, step);
			if (step >= stepToCount)
				currentCount++;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (currentCount > 0) {
			generateOutput(actionOutput);
		}
	}

	private void prepareForNewTuple(Tuple tuple) {
		tl1.setValue(((TLong) tuple.get(0)).getValue());
		tl2.setValue(((TLong) tuple.get(1)).getValue());
		tl3.setValue(((TLong) tuple.get(2)).getValue());
		minStep = ((TInt) tuple.get(3)).getValue();
		if (minStep >= stepToCount) {
			currentCount = 1;
		} else {
			currentCount = 0;
		}
	}

	private void generateOutput(ActionOutput actionOutput) throws Exception {
		step.setValue(minStep);
		count.setValue(currentCount);
		actionOutput.output(outputTuple);
	}

	private boolean sameTriple(Tuple t) {
		return t.get(0).equals(tl1) && t.get(1).equals(tl2)
				&& t.get(2).equals(tl3);
	}
}
