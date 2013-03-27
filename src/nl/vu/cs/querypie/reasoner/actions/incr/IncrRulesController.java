package nl.vu.cs.querypie.reasoner.actions.incr;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class IncrRulesController extends Action {
	public static final int S_DELTA_DIR = 0;
	public static final int ADD = 1;

	private boolean add;
	private String deltaDir;

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DELTA_DIR, "dir of the update", null, true);
		conf.registerParameter(ADD, "add or remove", true, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		deltaDir = getParamString(S_DELTA_DIR);
		add = getParamBoolean(ADD);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		TupleSet currentDelta = ActionsHelper
				.populateInMemorySetFromFile(deltaDir);
		TupleSet completeDelta = new TupleSetImpl();
		completeDelta.addAll(currentDelta);
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
		context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
		List<ActionConf> actions = new ArrayList<ActionConf>();
		List<ActionConf> actionsToBranch = new ArrayList<ActionConf>();
		// Initialization: one step derivation from the in-memory delta (set to
		// add/remove)
		ActionsHelper.runOneStepRulesControllerToMemory(actions);
		ActionsHelper.collectToNode(actions);
		if (add) {
			ActionsHelper.readAllInMemoryTuples(actionsToBranch,
					Consts.CURRENT_DELTA_KEY);
			ActionsHelper.runIncrAddController(actionsToBranch);
			ActionsHelper.createBranch(actions, actionsToBranch);
		} else {
			ActionsHelper.readAllInMemoryTuples(actionsToBranch,
					Consts.CURRENT_DELTA_KEY);
			ActionsHelper.runIncrRemoveController(actionsToBranch);
			ActionsHelper.createBranch(actions, actionsToBranch);
		}
		actionOutput.branch(actions);
	}

}
