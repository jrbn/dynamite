package nl.vu.cs.querypie.reasoner.actions.common;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class ParallelExecutionSchemaOnly extends Action {
	public static void addToChain(int minimumStep, int outputStep, ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(ParallelExecutionSchemaOnly.class);
		a.setParamInt(I_MINIMUM_STEP, minimumStep);
		a.setParamInt(I_OUTPUT_STEP, outputStep);
		actions.add(a);
	}

	public static final int I_MINIMUM_STEP = 0;
	public static final int I_OUTPUT_STEP = 1;

	private int minimumStep;
	private int outputStep;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_MINIMUM_STEP, "I_MINIMUM_STEP", Integer.MIN_VALUE, true);
		conf.registerParameter(I_OUTPUT_STEP, "I_OUTPUT_STEP", Integer.MIN_VALUE, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		minimumStep = getParamInt(I_MINIMUM_STEP);
		outputStep = getParamInt(I_OUTPUT_STEP);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionsHelper.executeAllSchemaRulesInParallel(minimumStep, outputStep, false, actionOutput);
	}
}
