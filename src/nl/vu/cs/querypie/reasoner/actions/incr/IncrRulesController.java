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
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMapImpl;

public class IncrRulesController extends Action {
	public static final int S_DELTA_DIR = 0;
	public static final int B_ADD = 1;
	public static final int B_COUNT_DERIVATIONS = 2;
	public static final int I_LAST_STEP = 3;

	private boolean add;
	private String deltaDir;
	private boolean countDerivations;
	private int lastStep;

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {

	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DELTA_DIR, "dir of the update", null, true);
		conf.registerParameter(B_ADD, "add or remove", true, false);
		conf.registerParameter(B_COUNT_DERIVATIONS, "count_derivations", false, true);
		conf.registerParameter(I_LAST_STEP, "last_step", 0, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		deltaDir = getParamString(S_DELTA_DIR);
		add = getParamBoolean(B_ADD);
		countDerivations = getParamBoolean(B_COUNT_DERIVATIONS);
		lastStep = getParamInt(I_LAST_STEP);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		TupleSet currentDelta = ActionsHelper.populateInMemorySetFromFile(deltaDir);
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
		if (countDerivations) {
			TupleStepMap completeDelta = new TupleStepMapImpl();
			for (Tuple t : currentDelta) {
				completeDelta.put(t, 1);
			}
			context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
		} else {
			TupleSet completeDelta = new TupleSetImpl();
			completeDelta.addAll(currentDelta);
			context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
		}
		List<ActionConf> actions = new ArrayList<ActionConf>();
		List<ActionConf> actionsToBranch = new ArrayList<ActionConf>();
		// Initialization: one step derivation from the in-memory delta (set to
		// add/remove)
		ActionsHelper.runOneStepRulesControllerToMemory(actions);
		ActionsHelper.collectToNode(actions);
		if (add) {
			ActionsHelper.readAllInMemoryTuples(actionsToBranch, Consts.CURRENT_DELTA_KEY);
			ActionsHelper.runIncrAddController(actionsToBranch, lastStep + 1);
			ActionsHelper.createBranch(actions, actionsToBranch);
		} else {
			ActionsHelper.readAllInMemoryTuples(actionsToBranch, Consts.CURRENT_DELTA_KEY);
			ActionsHelper.runIncrRemoveController(actionsToBranch);
			ActionsHelper.createBranch(actions, actionsToBranch);
		}
		actionOutput.branch((ActionConf[]) actions.toArray());
	}

}
