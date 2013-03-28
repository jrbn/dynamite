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
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.io.RemoveDerivationsBtree;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class IncrRemoveDuplController extends Action {

	public static void addToChain(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(IncrRemoveDuplController.class);
		actions.add(c);
	}

	private TupleSet currentDelta;

	private TupleStepMap completeDelta;
	private Tuple currentTuple;

	private void executeOneForwardChainIterationAndRestart(ActionContext context, ActionOutput actionOutput) throws Exception {
		List<ActionConf> actions = new ArrayList<ActionConf>();
		IncrRulesParallelExecution.addToChain(actions);
		ActionsHelper.collectToNode(actions);
		ActionsHelper.removeDuplicates(actions);
		IncrRemoveDuplController.addToChain(actions);
		actionOutput.branch((ActionConf[]) actions.toArray());
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		tuple.copyTo(currentTuple);
		completeDelta.put(currentTuple, 1);

		if (!completeDelta.containsKey(currentTuple)) {
			currentDelta.add(currentTuple);
			currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
		}
	}

	private void removeDerivationsFromBtree(ActionContext context, ActionOutput actionOutput) throws Exception {
		List<ActionConf> actions = new ArrayList<ActionConf>();
		ActionsHelper.readFakeTuple(actions);
		ActionConf c = ActionFactory.getActionConf(RemoveDerivationsBtree.class);
		actions.add(c);
		actionOutput.branch((ActionConf[]) actions.toArray());
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		currentDelta = new TupleSetImpl();
		completeDelta = (TupleStepMap) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		saveCurrentDelta(context);
		if (!currentDelta.isEmpty()) {
			// Repeat the process (execute a new iteration) considering the
			// current delta
			executeOneForwardChainIterationAndRestart(context, actionOutput);
		} else {
			// Remove the derivations from the B-tree
			removeDerivationsFromBtree(context, actionOutput);
		}
	}
}
