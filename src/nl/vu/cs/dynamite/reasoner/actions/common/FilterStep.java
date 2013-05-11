package nl.vu.cs.dynamite.reasoner.actions.common;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;

public class FilterStep extends Action {

	public static final int I_STEP = 0;

	private int step;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", null, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		step = getParamInt(I_STEP);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (((TInt) tuple.get(3)).getValue() >= step) {
			actionOutput.output(tuple);
		}
	}

}
