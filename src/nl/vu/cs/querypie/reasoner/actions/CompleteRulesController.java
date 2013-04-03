package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.io.MemoryStorage;

/**
 * A rules controller that execute the complete materialization of all the
 * tuples based on the facts written on the knowledge base and on the derivation
 * rules.
 */
public class CompleteRulesController extends AbstractRulesController {
	public static final int I_STEP = 0;

	public static void addToChain(ActionSequence actions) throws ActionNotConfiguredException {
		addToChain(actions, 1);
	}

	public static void addToChain(ActionSequence actions, int step) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(CompleteRulesController.class);
		c.setParamInt(CompleteRulesController.I_STEP, step);
		actions.add(c);
	}

	private boolean hasDerived;
	private int step;

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		hasDerived = true;
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		hasDerived = false;
		step = getParamInt(I_STEP);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		if (!hasDerived)
			return;
		context.incrCounter("Iterations", 1);
		ActionSequence actions = new ActionSequence();
		if (!ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().isEmpty()) {
			applyRulesSchemaOnly(actions, MemoryStorage.BTREE, step);
			applyRulesWithGenericPatternsInABranch(actions, MemoryStorage.BTREE, step + 1);
		} else {
			applyRulesWithGenericPatterns(actions, MemoryStorage.BTREE, step + 1);
		}
		ActionsHelper.collectToNode(actions);
		CompleteRulesController.addToChain(actions, step + 3);
		actionOutput.branch(actions);
	}

}
