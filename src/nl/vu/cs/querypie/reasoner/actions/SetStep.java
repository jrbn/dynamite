package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class SetStep extends Action {
	public static void addToChain(int step, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(SetStep.class);
		c.setParamInt(SetStep.I_STEP, step);
		actions.add(c);
	}

	public static final int I_STEP = 0;
	private final SimpleData[] outputTuple = new SimpleData[4];

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		outputTuple[3] = new TInt(getParamInt(I_STEP));
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		outputTuple[0] = tuple.get(0);
		outputTuple[1] = tuple.get(1);
		outputTuple[2] = tuple.get(2);
		actionOutput.output(outputTuple);
	}

}
