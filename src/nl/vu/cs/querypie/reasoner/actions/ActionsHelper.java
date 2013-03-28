package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.actions.support.WritableListActions;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.io.ReadAllInMemoryTriples;
import nl.vu.cs.querypie.reasoner.actions.io.ReadFromBtree;
import nl.vu.cs.querypie.reasoner.actions.io.WriteDerivationsBtree;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecompGenericMap;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecompGenericReduce;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecomputedRuleExecutor;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ActionsHelper {

	public static void collectToNode(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		if (ParamHandler.get().isUsingCount()) {
			c.setParamStringArray(CollectToNode.TUPLE_FIELDS,
					TLong.class.getName(), TLong.class.getName(),
					TLong.class.getName(), TInt.class.getName());
		} else {
			c.setParamStringArray(CollectToNode.TUPLE_FIELDS,
					TLong.class.getName(), TLong.class.getName(),
					TLong.class.getName());
		}
		actions.add(c);
	}

	public static void createBranch(List<ActionConf> actions,
			List<ActionConf> actionsToBranch) {
		ActionConf c = ActionFactory.getActionConf(Branch.class);
		c.setParamWritable(Branch.BRANCH, new WritableListActions(
				actionsToBranch));
		actions.add(c);
	}

	private static void groupBy(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(GroupBy.class);
		c.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
		c.setParamStringArray(GroupBy.TUPLE_FIELDS, TByteArray.class.getName(),
				TBoolean.class.getName(), TByte.class.getName(),
				TLong.class.getName());
		c.setParamInt(GroupBy.NPARTITIONS_PER_NODE,
				nl.vu.cs.querypie.reasoner.common.Consts.GROUP_BY_NUM_THREADS);
		actions.add(c);
	}

	public static void mapReduce(List<ActionConf> actions, int minimumStep,
			boolean incrementalFlag) {
		PrecompGenericMap.addToChain(actions, minimumStep, incrementalFlag);
		groupBy(actions);
		PrecompGenericReduce.addToChain(actions, minimumStep, incrementalFlag);
	}

	static void parallelRunPrecomputedRuleExecutorForAllRules(int step,
			int numRules, boolean incrementalFlag, ActionOutput actionOutput)
			throws Exception {
		for (int ruleId = 0; ruleId < numRules; ++ruleId) {
			List<ActionConf> actions = new ArrayList<ActionConf>();
			readFakeTuple(actions);
			runPrecomputedRuleExecutorForRule(step, ruleId, actions,
					incrementalFlag);
			actionOutput
					.branch(actions.toArray(new ActionConf[actions.size()]));
		}
	}

	public static void parallelRunPrecomputedRuleExecutorForRules(
			List<Integer> ruleIds, boolean incrementalFlag,
			ActionOutput actionOutput) throws Exception {
		for (Integer ruleId : ruleIds) {
			List<ActionConf> actions = new ArrayList<ActionConf>();
			readFakeTuple(actions);
			runPrecomputedRuleExecutorForRule(Integer.MIN_VALUE, ruleId,
					actions, incrementalFlag);
			actionOutput.branch((ActionConf[]) actions.toArray());
		}
	}

	public static void readEverythingFromBTree(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
		c.setParamInt(ReadFromBtree.PARALLEL_TASKS,
				nl.vu.cs.querypie.reasoner.common.Consts.READ_NUM_THREADS);
		c.setParamWritable(ReadFromBtree.TUPLE, new Query(new TLong(-1),
				new TLong(-1), new TLong(-1)));
		actions.add(c);
	}

	public static void readFakeTuple(List<ActionConf> actions) {
		ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
		a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
		actions.add(a);
	}

	public static void reconnectAfter(int reconnectAfter,
			List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, reconnectAfter);
		actions.add(c);
	}

	public static void reloadPrecomputationOnRules(Collection<Rule> rules,
			ActionContext context, boolean incrementalFlag) {
		for (Rule r : rules) {
			r.reloadPrecomputation(ReasoningContext.getInstance(), context,
					incrementalFlag);
		}
	}

	public static void removeDuplicates(List<ActionConf> actions) {
		actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));
	}

	public static void runPrecomputedRuleExecutorForRule(int step, int ruleId,
			List<ActionConf> actions, boolean incrementalFlag) {
		ActionConf a = ActionFactory
				.getActionConf(PrecomputedRuleExecutor.class);
		a.setParamInt(PrecomputedRuleExecutor.RULE_ID, ruleId);
		a.setParamBoolean(PrecomputedRuleExecutor.INCREMENTAL_FLAG,
				incrementalFlag);
		a.setParamInt(PrecomputedRuleExecutor.I_STEP, step);
		actions.add(a);
	}

	static void sort(List<ActionConf> actions, boolean additionalStepCounter) {
		ActionConf c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE,
				nl.vu.cs.querypie.reasoner.common.Consts.SORT_NUM_THREADS);
		if (additionalStepCounter) {
			c.setParamStringArray(PartitionToNodes.TUPLE_FIELDS,
					TLong.class.getName(), TLong.class.getName(),
					TLong.class.getName(), TInt.class.getName());
		} else {
			c.setParamStringArray(PartitionToNodes.TUPLE_FIELDS,
					TLong.class.getName(), TLong.class.getName(),
					TLong.class.getName());
		}
		c.setParamBoolean(PartitionToNodes.SORT, true);
		actions.add(c);
	}

	public static void writeInMemoryTuplesToBTree(boolean forceStep, int step,
			ActionContext context, ActionOutput actionOutput, String inMemoryKey)
			throws Exception {
		List<ActionConf> actions = new ArrayList<ActionConf>();
		readFakeTuple(actions);
		ReadAllInMemoryTriples.addToChain(actions, inMemoryKey);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions, false);
		}
		WriteDerivationsBtree.addToChain(forceStep, step, actions);
		actionOutput.branch(actions.toArray(new ActionConf[actions.size()]));
	}
}
