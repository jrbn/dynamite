package nl.vu.cs.querypie.reasoner.actions;

import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.storage.Pattern;

public class ActionsHelper {

  static void runSchemaRulesInParallel(List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(ParallelExecutionSchemaOnly.class);
    actions.add(a);
  }

  static void readFakeTuple(List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
    a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
    a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
    actions.add(a);
  }

  static void readEverythingFromBTree(List<ActionConf> actions) {
    ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
    c.setParamInt(ReadFromBtree.PARALLEL_TASKS, 4);
    c.setParamWritable(ReadFromBtree.TUPLE, new Query(new TLong(-1), new TLong(-1), new TLong(-1)));
    actions.add(c);
  }

  static void reconnectAfter(int reconnectAfter, List<ActionConf> actions) {
    ActionConf c = ActionFactory.getActionConf(Split.class);
    c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, reconnectAfter);
    actions.add(c);
  }

  static void runMap(List<ActionConf> actions, boolean incrementalFlag) {
    ActionConf c = ActionFactory.getActionConf(PrecompGenericMap.class);
    c.setParamBoolean(PrecompGenericMap.INCREMENTAL_FLAG, incrementalFlag);
    actions.add(c);
  }

  static void runReduce(List<ActionConf> actions, boolean incrementalFlag) {
    ActionConf c = ActionFactory.getActionConf(PrecompGenericReduce.class);
    c.setParamBoolean(PrecompGenericReduce.INCREMENTAL_FLAG, incrementalFlag);
    actions.add(c);
  }

  static void runGroupBy(List<ActionConf> actions) {
    ActionConf c = ActionFactory.getActionConf(GroupBy.class);
    c.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
    c.setParamStringArray(GroupBy.TUPLE_FIELDS, TByteArray.class.getName(), TByte.class.getName(), TLong.class.getName());
    c.setParamInt(GroupBy.NPARTITIONS_PER_NODE, 4);
    actions.add(c);
  }

  static void runGenericRuleExecutor(List<ActionConf> actions) {
    ActionConf c = ActionFactory.getActionConf(GenericRuleExecutor.class);
    actions.add(c);
  }

  static void runPrecomputeRuleExectorForRule(int ruleId, List<ActionConf> actions, boolean incrementalFlag) {
    ActionConf a = ActionFactory.getActionConf(PrecomputedRuleExecutor.class);
    a.setParamInt(PrecomputedRuleExecutor.RULE_ID, ruleId);
    a.setParamBoolean(PrecomputedRuleExecutor.INCREMENTAL_FLAG, incrementalFlag);
    actions.add(a);
  }

  static void runSort(List<ActionConf> actions) {
    ActionConf c = ActionFactory.getActionConf(PartitionToNodes.class);
    c.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE, 4);
    c.setParamStringArray(PartitionToNodes.TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName());
    c.setParamBoolean(PartitionToNodes.SORT, true);
    actions.add(c);
  }

  static void runRemoveDuplicates(List<ActionConf> actions) {
    actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));
  }

  static void runWriteDerivationsOnBTree(List<ActionConf> actions) {
    actions.add(ActionFactory.getActionConf(WriteDerivationsBtree.class));
  }

  static void runReloadSchema(List<ActionConf> actions, boolean incrementalFlag) {
    ActionConf c = ActionFactory.getActionConf(ReloadSchema.class);
    c.setParamBoolean(ReloadSchema.INCREMENTAL_FLAG, incrementalFlag);
    actions.add(c);
  }

  static void runCollectToNode(List<ActionConf> actions) {
    ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
    c.setParamStringArray(CollectToNode.TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName());
    actions.add(c);
  }

  static void runReadAllInMemoryTuples(List<ActionConf> actions) {
    actions.add(ActionFactory.getActionConf(ReadAllInmemoryTriples.class));
  }

  static void runRulesController(List<ActionConf> actions) {
    ActionConf c = ActionFactory.getActionConf(RulesController.class);
    actions.add(c);
  }

  static void runIncrRulesParallelExecution(List<ActionConf> actions) {
    actions.add(ActionFactory.getActionConf(IncrRulesParallelExecution.class));
  }

  static void runIncrRulesControllerInStage(int stage, List<ActionConf> actions, String deltaDir) {
    ActionConf c = ActionFactory.getActionConf(IncrRulesController.class);
    c.setParamInt(IncrRulesController.I_STAGE, stage);
    c.setParamString(IncrRulesController.S_DELTA_DIR, deltaDir);
    actions.add(c);
  }

  static void runReadFromBTree(Pattern pattern, List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(ReadFromBtree.class);
    Query query = new Query(new TLong(pattern.getTerm(0).getValue()), new TLong(pattern.getTerm(1).getValue()), new TLong(pattern.getTerm(2).getValue()));
    a.setParamWritable(ReadFromBtree.TUPLE, query);
    actions.add(a);
  }
}
