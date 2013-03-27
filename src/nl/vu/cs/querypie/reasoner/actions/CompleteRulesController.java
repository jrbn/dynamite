package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
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
	public static final int B_COUNT_DERIVATIONS = 1;

	private boolean hasDerived;
	private int step;
	private boolean countDerivations;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", null, true);
		conf.registerParameter(B_COUNT_DERIVATIONS, "count_derivations", false,
				true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		hasDerived = false;
		step = getParamInt(I_STEP);
		countDerivations = getParamBoolean(B_COUNT_DERIVATIONS);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		hasDerived = true;
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
			applyRulesSchemaOnly(actions, true, countDerivations, step);
			applyRulesWithGenericPatternsInABranch(actions, true,
					countDerivations, step + 1);
		} else {
			applyRulesWithGenericPatterns(actions, true, countDerivations,
					step + 1);
		}
		ActionsHelper.collectToNode(actions);
		ActionsHelper.runCompleteRulesController(actions, countDerivations,
				step + 3);
		actionOutput.branch(actions);
	}

}
