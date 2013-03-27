package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;

/**
 * A rules controller that execute a single step of materialization based on the
 * facts written in memory and on the derivation rules.
 * 
 * It writes the newly derived rules in memory (in a cached object)
 */
public class OneStepRulesControllerFromMemory extends Action {

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		List<ActionConf> actions = new ArrayList<ActionConf>();
		ActionsHelper.runIncrRulesParallelExecution(actions);
		ActionsHelper.collectToNode(actions);
		ActionsHelper.removeDuplicates(actions);
		actionOutput.branch((ActionConf[]) actions.toArray());
	}

}
