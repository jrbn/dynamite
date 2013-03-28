package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;

/**
 * A rules controller that execute the complete materialization of all the
 * tuples based on the facts written on the knowledge base and on the derivation
 * rules.
 */
public class CompleteRulesController extends AbstractRulesController {
	public static final int I_STEP = 0;

	public static void addToChain(List<ActionConf> actions) {
		addToChain(actions, 1);
	}

	public static void addToChain(List<ActionConf> actions, int step) {
		ActionConf c = ActionFactory
				.getActionConf(CompleteRulesController.class);
		c.setParamInt(CompleteRulesController.I_STEP, step);
		actions.add(c);
	}

	private boolean hasDerived;
	private int step;

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
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
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (!hasDerived)
			return;
		context.incrCounter("Iterations", 1);
		List<ActionConf> actions = new ArrayList<ActionConf>();
		if (!ReasoningContext.getInstance().getRuleset()
				.getAllSchemaOnlyRules().isEmpty()) {
			applyRulesSchemaOnly(actions, true, step, false);
			applyRulesWithGenericPatternsInABranch(actions, true, step + 1,
					false);
		} else {
			applyRulesWithGenericPatterns(actions, true, step + 1, false);
		}
		ActionsHelper.collectToNode(actions);
		CompleteRulesController.addToChain(actions, step + 3);
		actionOutput.branch(actions.toArray(new ActionConf[actions.size()]));
	}

}
