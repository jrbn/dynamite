package nl.vu.cs.querypie.reasoner.actions.controller;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.incr.IncrAddController;
import nl.vu.cs.querypie.reasoner.actions.incr.IncrRemoveController;
import nl.vu.cs.querypie.reasoner.actions.io.IOHelper;
import nl.vu.cs.querypie.reasoner.support.Consts;
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMapImpl;

public class IncrRulesController extends Action {
	public static void addToChain(ActionSequence actions, String deltaDir, boolean add) throws ActionNotConfiguredException {
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
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DELTA_DIR, "S_DELTA_DIR", null, true);
		conf.registerParameter(B_ADD, "B_ADD", true, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		deltaDir = getParamString(S_DELTA_DIR);
		add = getParamBoolean(B_ADD);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {

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
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readFakeTuple(actions);
		if (add) {
			IncrAddController.addToChain(ParamHandler.get().getLastStep(), true, actions);
		} else {
			IncrRemoveController.addToChain(true, actions);
		}
		actionOutput.branch(actions);
	}
}
