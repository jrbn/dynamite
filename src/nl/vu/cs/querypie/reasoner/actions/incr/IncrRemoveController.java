package nl.vu.cs.querypie.reasoner.actions.incr;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class IncrRemoveController extends Action {
	public static void addToChain(List<ActionConf> actions, boolean firstIteration) {
		ActionConf c = ActionFactory.getActionConf(IncrRemoveController.class);
		c.setParamBoolean(B_FIRST_ITERATION, firstIteration);
		actions.add(c);
	}

	public static final int B_FIRST_ITERATION = 0;

	private boolean firstIteration;
	private TupleSet currentDelta;
	private TupleSet completeDelta;
	private Tuple currentTuple;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_FIRST_ITERATION, "first iteration", true, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		currentDelta = new TupleSetImpl();
		completeDelta = (TupleSetImpl) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		if (firstIteration) {
			return;
		}
		tuple.copyTo(currentTuple);
		completeDelta.add(currentTuple);
		if (!completeDelta.contains(currentTuple)) {
			currentDelta.add(currentTuple);
			currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		if (firstIteration) {
			executeOneForwardChainIterationAndRestart(context, actionOutput);
		} else {
			saveCurrentDelta(context);
			if (!currentDelta.isEmpty()) {
				// Repeat the process (execute a new iteration) considering the
				// current delta
				executeOneForwardChainIterationAndRestart(context, actionOutput);
			} else {
				// Move to the second stage of the algorithm.
				List<ActionConf> actions = new ArrayList<ActionConf>();
				List<ActionConf> actionsToBranch = new ArrayList<ActionConf>();
				removeAllInMemoryTuplesFromBTree(context);
				// TODO make sure that information in memory is correct
				ActionsHelper.readFakeTuple(actionsToBranch);
				IncrAddController.addToChain(actionsToBranch, -1, true);
				ActionsHelper.createBranch(actions, actionsToBranch);
				actionOutput.branch(actions.toArray(new ActionConf[actions.size()]));
			}
		}
	}

	private void executeOneForwardChainIterationAndRestart(ActionContext context, ActionOutput actionOutput) throws Exception {
		List<ActionConf> actions = new ArrayList<ActionConf>();
		IncrRulesParallelExecution.addToChain(actions);
		ActionsHelper.collectToNode(actions);
		ActionsHelper.removeDuplicates(actions);
		IncrRemoveController.addToChain(actions, false);
		actionOutput.branch(actions.toArray(new ActionConf[actions.size()]));
	}

	private void removeAllInMemoryTuplesFromBTree(ActionContext context) {
		TupleSet set = (TupleSet) context.getObjectFromCache(Consts.CURRENT_DELTA_KEY);
		BerkeleydbLayer db = ReasoningContext.getInstance().getKB();
		for (Tuple t : set) {
			db.remove(t);
		}
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

}
