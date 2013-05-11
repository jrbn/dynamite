package nl.vu.cs.dynamite.reasoner.actions.incr;

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
import nl.vu.cs.dynamite.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.dynamite.reasoner.actions.common.ReplaceSteps;
import nl.vu.cs.dynamite.reasoner.actions.io.ReadAllInMemoryTriples;
import nl.vu.cs.dynamite.reasoner.actions.io.WriteDerivationsAllBtree;
import nl.vu.cs.dynamite.reasoner.support.Consts;
import nl.vu.cs.dynamite.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class IncrAddController extends Action {
	public static void addToChain(int step, boolean firstIteration,
			ActionSequence actions) throws ActionNotConfiguredException {
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
		conf.registerParameter(I_STEP, "I_STEP", 0, true);
		conf.registerParameter(B_FIRST_ITERATION, "B_FIRST_ITERATION", true,
				false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		step = getParamInt(I_STEP);
		firstIteration = getParamBoolean(B_FIRST_ITERATION);
		currentDelta = new TupleSetImpl();
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
				new TLong(), new TInt());
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (firstIteration) {
			return;
		}
		tuple.copyTo(currentTuple);
		Object completeDeltaObj = context
				.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		if (completeDeltaObj instanceof TupleSet) {
			TupleSet completeDelta = (TupleSet) completeDeltaObj;
			if (completeDelta.add(currentTuple)) {
				currentDelta.add(currentTuple);
				currentTuple = TupleFactory.newTuple();
			}
		} else {
			TupleStepMap completeDelta = (TupleStepMap) completeDeltaObj;
			if (!completeDelta.containsKey(tuple)) {
				currentDelta.add(currentTuple);
			}
			completeDelta.put(currentTuple, 1);
			currentTuple = TupleFactory.newTuple();
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// In case of new derivations, perform another iteration
		if (firstIteration) {
			executeOneForwardChainingIterationAndRestart(context, actionOutput);
		} else if (!currentDelta.isEmpty()) {
			saveCurrentDelta(context);
			executeOneForwardChainingIterationAndRestart(context, actionOutput);
		} else {
			// Otherwise stop and write complete derivations on btree
			ParamHandler.get().setLastStep(step);

			ActionSequence actions = new ActionSequence();
			ActionsHelper.readFakeTuple(actions);
			ReadAllInMemoryTriples.addToChain(Consts.COMPLETE_DELTA_KEY,
					actions);

			// If There is a temporary cache that contains the steps use it to
			// replace the correct steps.
			if (context.getObjectFromCache(Consts.TMP_REMOVALS) != null) {
				actions.add(ActionFactory.getActionConf(ReplaceSteps.class));
			}

			WriteDerivationsAllBtree.addToChain(actions);
			actionOutput.branch(actions);
		}
	}

	private void executeOneForwardChainingIterationAndRestart(
			ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		IncrRulesParallelExecution.addToChain(step, actions);
		ActionsHelper.collectToNode(false, actions);
		IncrAddController.addToChain(step + 1, false, actions);
		actionOutput.branch(actions);
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}
}
