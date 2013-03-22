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

  static final int S_DELTA_DIR = 0;
  static final int I_STAGE = 1;
  static final Logger log = LoggerFactory.getLogger(IncrRulesController.class);

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
      break;
    case 1:
    case 2:
      completeDelta = (InMemoryTupleSet) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
      currentDelta = new InMemoryTreeTupleSet();
      currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
      break;
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    switch (stage) {
    case 1:
    case 2:
      tuple.copyTo(currentTuple);
      if (!completeDelta.contains(currentTuple)) {
        currentDelta.add(currentTuple);
        currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
      }
      break;
    }
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
    case 2:
      stop2(context, actionOutput);
      break;
    }
  }

  private void stop0(ActionContext context, ActionOutput actionOutput) throws Exception {
    executeOneForwardChainIterationAndRestartFromStage(1, context, actionOutput);
  }

  private void stop1(ActionContext context, ActionOutput actionOutput) throws Exception {
    if (!currentDelta.isEmpty()) {
      // Repeat the process (execute a new iteration) considering the current delta
      executeOneForwardChainIterationAndRestartFromStage(1, context, actionOutput);
    } else {
      // Move to the second stage of the algorithm.
      List<ActionConf> actions = new ArrayList<ActionConf>();
      ActionsHelper.runRemoveAllInMemoryTriplesFromBTree(actions, Consts.COMPLETE_DELTA_KEY);
      ActionsHelper.runRemoveCleanInMemoryTriples(actions, Consts.CURRENT_DELTA_KEY);
      ActionsHelper.runRemoveCleanInMemoryTriples(actions, Consts.COMPLETE_DELTA_KEY);
      ActionsHelper.runIncrRulesControllerInStage(2, actions, deltaDir);
      actionOutput.branch(actions);
    }
  }

  private void stop2(ActionContext context, ActionOutput actionOutput) throws Exception {
    if (!currentDelta.isEmpty()) {
      executeOneForwardChainIterationAndRestartFromStage(2, context, actionOutput);
    }
  }

  private void executeOneForwardChainIterationAndRestartFromStage(int stage, ActionContext context, ActionOutput actionOutput) throws Exception {
    List<ActionConf> actions = new ArrayList<ActionConf>();
    executeOneForwardChainIterationAndRestartFromStage(stage, context, actions);
    actionOutput.branch(actions);
  }

  private void executeOneForwardChainIterationAndRestartFromStage(int stage, ActionContext context, List<ActionConf> actions) throws Exception {
    saveCurrentDelta(context);
    updateAndSaveCompleteDelta(context);
    ActionsHelper.runIncrRulesParallelExecution(actions);
    ActionsHelper.runCollectToNode(actions);
    ActionsHelper.runRemoveDuplicates(actions);
    ActionsHelper.runIncrRulesControllerInStage(stage, actions, deltaDir);
  }

  private void readDeltaFromFileAndPutItInCache(ActionContext context) {
    try {
      currentDelta = ActionsHelper.populateInMemorySetFromFile(deltaDir);
    } catch (Exception e) {
      log.error("Error retrieving information from file");
    }
    completeDelta = new InMemoryTreeTupleSet();
    completeDelta.addAll(currentDelta);
    context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
    context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
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
