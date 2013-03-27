package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.actions.incr.IncrRulesParallelExecution;

/**
 * A rules controller that execute a single step of materialization based on the
 * facts written in memory and on the derivation rules.
 * 
 * It writes the newly derived rules in memory (in a cached object)
 */
public class OneStepRulesControllerFromMemory extends Action {
	public static void addToChain(List<ActionConf> actions) {
		ActionConf a = ActionFactory.getActionConf(OneStepRulesControllerFromMemory.class);
		actions.add(a);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		List<ActionConf> actions = new ArrayList<ActionConf>();
		IncrRulesParallelExecution.addToChain(actions);
		ActionsHelper.collectToNode(actions);
		ActionsHelper.removeDuplicates(actions);
		actionOutput.branch((ActionConf[]) actions.toArray());
	}

}
