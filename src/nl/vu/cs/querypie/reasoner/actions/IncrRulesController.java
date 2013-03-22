package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTreeTupleSet;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrRulesController extends Action {
  static final Logger log = LoggerFactory.getLogger(IncrRulesController.class);

  static final int S_DELTA_DIR = 0;
  static final int I_STAGE = 1;

  private String deltaDir = null;
  private int stage = 0;
  private InMemoryTupleSet currentDelta = null;
  private InMemoryTupleSet completeDelta = null;
  private Tuple currentTuple = null;

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(S_DELTA_DIR, "dir of the update", null, true);
    conf.registerParameter(I_STAGE, "stage computation", 0, false);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    deltaDir = getParamString(S_DELTA_DIR);
    stage = getParamInt(I_STAGE);
    switch (stage) {
    case 0:
      readDeltaFromFileAndPutItInCache(context);
    case 1:
      completeDelta = (InMemoryTupleSet) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
      currentDelta = new InMemoryTreeTupleSet();
      currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
      break;
    }
  }

  private void process1(Tuple tuple, ActionContext context, ActionOutput actionOutput) {
    tuple.copyTo(currentTuple);
    if (!completeDelta.contains(currentTuple)) {
      currentDelta.add(currentTuple);
      currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    switch (stage) {
    case 1:
      process1(tuple, context, actionOutput);
      break;
    }
  }

  private void stop0(ActionContext context, ActionOutput actionOutput) throws Exception {
    executeOneForwardChainIteration(context, actionOutput);
  }

  private void stop1(ActionContext context, ActionOutput actionOutput) throws Exception {
    if (!currentDelta.isEmpty()) {
      // Repeat the process (execute a new iteration) considering the current delta
      saveCurrentDelta(context);
      executeOneForwardChainIteration(context, actionOutput);
    } else {
      // TODO: Move to the second stage of the algorithm.
      // 1) Remove everything in Delta
      // 2) Recompute the remaining derivation
    }
    currentDelta = null;
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    switch (stage) {
    case 0:
      stop0(context, actionOutput);
      break;
    case 1:
      stop1(context, actionOutput);
      break;
    }
  }

  private void executeOneForwardChainIteration(ActionContext context, ActionOutput actionOutput) throws Exception {
    List<ActionConf> actions = new ArrayList<ActionConf>();
    ActionsHelper.runIncrRulesParallelExecution(actions);
    ActionsHelper.runCollectToNode(actions);
    ActionsHelper.runRemoveDuplicates(actions);
    updateAndSaveCompleteDelta(context);
    ActionsHelper.runIncrRulesControllerInStage(1, actions, deltaDir);
    actionOutput.branch(actions);
  }

  private void readDeltaFromFileAndPutItInCache(ActionContext context) {
    InMemoryTupleSet delta = null;
    try {
      delta = ActionsHelper.parseTriplesFromFile(deltaDir);
    } catch (Exception e) {
      log.error("Error retrieving information from file");
    }
    context.putObjectInCache(Consts.CURRENT_DELTA_KEY, delta);
  }

  private void updateAndSaveCompleteDelta(ActionContext context) {
    completeDelta = (InMemoryTupleSet) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
    if (completeDelta == null) {
      completeDelta = new InMemoryTreeTupleSet();
    }
    completeDelta.addAll(currentDelta);
    context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
  }

  private void saveCurrentDelta(ActionContext context) {
    context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
  }

}