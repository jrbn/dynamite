package nl.vu.cs.querypie.reasoner.actions;

import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

public class AddDerivationCount extends Action {
	public static void addToChain(List<ActionConf> actions, boolean groupSteps) {
		ActionConf c = ActionFactory.getActionConf(AddDerivationCount.class);
		c.setParamBoolean(AddDerivationCount.B_GROUP_STEPS, groupSteps);
		actions.add(c);
	}

	public static final int B_GROUP_STEPS = 0;

	private boolean first = true;
	private int currentCount = 0;

	private SimpleData[] outputTuple;
	private Tuple previousTuple;
	private TInt refCount;
	private TInt refStep;

	private boolean countStep;
	private int minStep;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_GROUP_STEPS, "group", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {

		countStep = getParamBoolean(B_GROUP_STEPS);
		if (countStep) {
			outputTuple = new SimpleData[5];
			outputTuple[4] = refStep = new TInt();
		} else {
			outputTuple = new SimpleData[4];
		}
		outputTuple[0] = new TLong();
		outputTuple[1] = new TLong();
		outputTuple[2] = new TLong();
		outputTuple[3] = refCount = new TInt();

		previousTuple = TupleFactory.newTuple(outputTuple);
		first = true;
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		if (first) {
			first = false;
			currentCount = 1;
			minStep = Integer.MAX_VALUE;
			tuple.copyTo(previousTuple);

			if (countStep) {
				int cv = ((TInt) tuple.get(3)).getValue();
				if (cv < minStep) {
					cv = minStep;
				}
			}

		} else if (!previousTuple.equals(tuple)) {
			refCount.setValue(currentCount);
			if (countStep) {
				refStep.setValue(minStep);
			}
			actionOutput.output(outputTuple);
			currentCount = 1;
			minStep = Integer.MAX_VALUE;
			tuple.copyTo(previousTuple);

			if (countStep) {
				int cv = ((TInt) tuple.get(3)).getValue();
				if (cv < minStep) {
					cv = minStep;
				}
			}
		} else {
			currentCount++;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		if (!first) {
			refCount.setValue(currentCount);
			if (countStep) {
				refStep.setValue(minStep);
			}
			actionOutput.output(outputTuple);
		}
	}
}
