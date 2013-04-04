package nl.vu.cs.querypie.reasoner.actions.incr;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class IncrAddController extends Action {
	public static void addToChain(ActionSequence actions, int step, boolean firstIteration) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(IncrAddController.class);
		c.setParamInt(IncrAddController.I_STEP, step);
		c.setParamBoolean(IncrAddController.B_FIRST_ITERATION, firstIteration);
		actions.add(c);
	}

	public static final int I_STEP = 0;
	public static final int B_FIRST_ITERATION = 1;

	private TupleSet currentDelta;
	private Tuple currentTuple;

	private int step;
	private boolean firstIteration;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", 0, true);
		conf.registerParameter(B_FIRST_ITERATION, "first_iteration", true, false);
	}

	private void executeAForwardChainingIterationAndRestart(ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		IncrRulesParallelExecution.addToChain(actions);
		ActionsHelper.collectToNode(actions, false);
		if (!ParamHandler.get().isUsingCount()) {
			ActionsHelper.removeDuplicates(actions);
		}
		IncrAddController.addToChain(actions, step, false);
		actionOutput.branch(actions);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		step = getParamInt(I_STEP);
		firstIteration = getParamBoolean(B_FIRST_ITERATION);
		currentDelta = new TupleSetImpl();
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong(), new TInt());
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		if (!firstIteration) {
			tuple.copyTo(currentTuple);
			Object completeDeltaObj = context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
			if (completeDeltaObj instanceof TupleSet) {
				TupleSet completeDelta = (TupleSet) completeDeltaObj;
				if (!completeDelta.contains(currentTuple)) {
					currentDelta.add(currentTuple);
					completeDelta.add(currentTuple);
					currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong(), new TInt());
				}
			} else {
				TupleStepMap completeDelta = (TupleStepMap) completeDeltaObj;
				if (!completeDelta.containsKey(tuple)) {
					currentDelta.add(currentTuple);
				}
				completeDelta.put(currentTuple, 1);
				currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong(), new TInt());
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		// In case of new derivations, perform another iteration
		if (firstIteration) {
			executeAForwardChainingIterationAndRestart(context, actionOutput);
		} else if (!currentDelta.isEmpty()) {
			saveCurrentDelta(context);
			executeAForwardChainingIterationAndRestart(context, actionOutput);
		}
		// Otherwise stop and write complete derivations on btree
		else {
			writeCompleteDeltaToBTree(context, actionOutput);
		}
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

	private void writeCompleteDeltaToBTree(ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionsHelper.writeInMemoryTuplesToBTree(step, context, actionOutput, Consts.COMPLETE_DELTA_KEY);
	}

}
