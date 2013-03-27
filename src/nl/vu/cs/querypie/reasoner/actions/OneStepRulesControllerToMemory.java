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
 * A rules controller that execute a single step of materialization based on the
 * facts written on the knowledge base and on the derivation rules.
 * 
 * It writes the newly derived rules in memory (in a cached object)
 */
public class OneStepRulesControllerToMemory extends AbstractRulesController {
	public static void addToChain(List<ActionConf> actions) {
		ActionConf a = ActionFactory
				.getActionConf(OneStepRulesControllerToMemory.class);
		actions.add(a);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		List<ActionConf> actions = new ArrayList<ActionConf>();
		if (!ReasoningContext.getInstance().getRuleset()
				.getAllSchemaOnlyRules().isEmpty()) {
			applyRulesSchemaOnly(actions, false, Integer.MIN_VALUE, true);
			applyRulesWithGenericPatternsInABranch(actions, false,
					Integer.MIN_VALUE, true);
		} else {
			applyRulesWithGenericPatterns(actions, false, Integer.MIN_VALUE,
					true);
		}
		ActionsHelper.collectToNode(actions);
		actionOutput.branch(actions.toArray(new ActionConf[actions.size()]));
	}

}
