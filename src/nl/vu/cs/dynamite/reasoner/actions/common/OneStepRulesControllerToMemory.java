package nl.vu.cs.dynamite.reasoner.actions.common;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.dynamite.ReasoningContext;
import nl.vu.cs.dynamite.reasoner.actions.rules.GenericRuleExecutor;
import nl.vu.cs.dynamite.reasoner.support.Consts;
import nl.vu.cs.dynamite.storage.inmemory.TupleSet;
import nl.vu.cs.dynamite.storage.inmemory.TupleStepMap;

/**
 * A rules controller that execute a single step of materialization based on the
 * facts written on the knowledge base and on the derivation rules.
 * 
 * It writes the newly derived rules in memory (in a cached object)
 */
public class OneStepRulesControllerToMemory extends AbstractRulesController {

	public static final int I_STEP = 0;

	public static void addToChain(ActionSequence actions, int step)
			throws ActionNotConfiguredException {
		ActionConf a = ActionFactory
				.getActionConf(OneStepRulesControllerToMemory.class);
		a.setParamInt(I_STEP, step);
		actions.add(a);
	}

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", null, true);
	}

	private int currentStep;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		currentStep = getParamInt(I_STEP);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {

		cleanInMemoryContainer(context, Consts.COMPLETE_DELTA_KEY);
		cleanInMemoryContainer(context, Consts.CURRENT_DELTA_KEY);

		// Rules only with schema
		if (!ReasoningContext.getInstance().getRuleset()
				.getAllSchemaOnlyRules().isEmpty()) {
			ReasoningContext.getInstance().getRuleset()
					.reloadPrecomputationSchema(context, true, false);
			Set<Integer> schemaOnlyRules = new HashSet<Integer>();
			for (int i = 0; i < ReasoningContext.getInstance().getRuleset()
					.getAllSchemaOnlyRules().size(); ++i) {
				schemaOnlyRules.add(i);
			}
			ActionsHelper.executeSchemaRulesInParallel(schemaOnlyRules,
					Integer.MIN_VALUE, currentStep, false, actionOutput);
		}

		// Rules with generic tuples
		ReasoningContext.getInstance().getRuleset()
				.reloadPrecomputationSchemaGeneric(context, true, false);
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readEverythingFromBTree(actions);
		if (!ReasoningContext.getInstance().getRuleset()
				.getAllRulesWithOneAntecedent().isEmpty()) {
			ActionsHelper.filterPotentialInput(7, actions);
			ActionsHelper.reconnectAfter(2, actions);
			GenericRuleExecutor.addToChain(Integer.MIN_VALUE, currentStep,
					actions);
			ActionsHelper.reconnectAfter(4, actions);
		} else {
			ActionsHelper.filterPotentialInput(4, actions);
		}
		ActionsHelper.mapReduce(Integer.MIN_VALUE, currentStep, false, actions);
		actionOutput.branch(actions);
	}

	private void cleanInMemoryContainer(ActionContext context, String key) {
		Object obj = context.getObjectFromCache(key);
		if (obj == null) {
			return;
		} else if (obj instanceof TupleSet) {
			((TupleSet) obj).clear();
		} else if (obj instanceof TupleStepMap) {
			((TupleStepMap) obj).clear();
		}
	}
}
