package nl.vu.cs.querypie.reasoner.actions.incr;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.OneStepRulesControllerToMemory;
import nl.vu.cs.querypie.reasoner.actions.io.IOHelper;
import nl.vu.cs.querypie.reasoner.actions.io.ReadAllInMemoryTriples;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMapImpl;

public class IncrRulesController extends Action {
	public static void addToChain(List<ActionConf> actions, String deltaDir, boolean add) {
		ActionConf a = ActionFactory.getActionConf(IncrRulesController.class);
		a.setParamString(IncrRulesController.S_DELTA_DIR, deltaDir);
		a.setParamBoolean(IncrRulesController.B_ADD, add);
		actions.add(a);
	}

	public static final int S_DELTA_DIR = 0;
	public static final int B_ADD = 1;

	private boolean add;
	private String deltaDir;

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {

	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DELTA_DIR, "dir of the update", null, true);
		conf.registerParameter(B_ADD, "add or remove", true, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		deltaDir = getParamString(S_DELTA_DIR);
		add = getParamBoolean(B_ADD);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		TupleSet currentDelta = IOHelper.populateInMemorySetFromFile(deltaDir);
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
		if (ParamHandler.get().isUsingCount()) {
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
		ActionsHelper.readFakeTuple(actions);
		OneStepRulesControllerToMemory.addToChain(actions);
		ActionsHelper.collectToNode(actions);
		if (add) {
			ActionsHelper.readFakeTuple(actionsToBranch);
			ReadAllInMemoryTriples.addToChain(actionsToBranch, Consts.CURRENT_DELTA_KEY);
			IncrAddController.addToChain(actionsToBranch, -1);
			ActionsHelper.createBranch(actions, actionsToBranch);
		} else {
			ActionsHelper.readFakeTuple(actionsToBranch);
			ReadAllInMemoryTriples.addToChain(actionsToBranch, Consts.CURRENT_DELTA_KEY);
			IncrRemoveController.addToChain(actionsToBranch);
			ActionsHelper.createBranch(actions, actionsToBranch);
		}
		actionOutput.branch((ActionConf[]) actions.toArray());
	}

}
