package nl.vu.cs.querypie.reasoner.actions;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
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

	public static void collectToNode(ActionSequence actions) throws ActionNotConfiguredException {
		collectToNode(actions, ParamHandler.get().isUsingCount());
	}

	public static void collectToNode(ActionSequence actions, boolean hasAdditionalField) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		if (hasAdditionalField) {
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName(), TInt.class.getName());
		} else {
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName());
		}
		actions.add(c);
	}

	public static void createBranch(ActionSequence actions, ActionSequence actionsToBranch) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(Branch.class);
		c.setParamWritable(Branch.W_BRANCH, actionsToBranch);
		actions.add(c);
	}

	private static void groupBy(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(GroupBy.class);
		c.setParamByteArray(GroupBy.IA_FIELDS_TO_GROUP, (byte) 0);
		c.setParamStringArray(GroupBy.SA_TUPLE_FIELDS, TByteArray.class.getName(), TBoolean.class.getName(), TByte.class.getName(), TLong.class.getName());
		c.setParamInt(GroupBy.I_NPARTITIONS_PER_NODE, nl.vu.cs.querypie.reasoner.common.Consts.GROUP_BY_NUM_THREADS);
		actions.add(c);
	}

	public static void mapReduce(ActionSequence actions, int minimumStep, boolean incrementalFlag) throws ActionNotConfiguredException {
		PrecompGenericMap.addToChain(actions, minimumStep, incrementalFlag);
		groupBy(actions);
		PrecompGenericReduce.addToChain(actions, minimumStep, incrementalFlag);
	}

	static void parallelRunPrecomputedRuleExecutorForAllRules(int step, boolean incrementalFlag, ActionOutput actionOutput) throws Exception {
		Set<Integer> schemaOnlyRules = new HashSet<Integer>();
		for (int i = 0; i < ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().size(); ++i) {
			schemaOnlyRules.add(i);
		}
		parallelRunPrecomputedRuleExecutorForRules(schemaOnlyRules, step, incrementalFlag, actionOutput);
	}

	public static void parallelRunPrecomputedRuleExecutorForRules(Set<Integer> ruleIds, int step, boolean incrementalFlag, ActionOutput actionOutput)
			throws Exception {
		for (Integer ruleId : ruleIds) {
			ActionSequence actions = new ActionSequence();
			readFakeTuple(actions);
			PrecomputedRuleExecutor.addToChain(step, ruleId, actions, incrementalFlag);
			actionOutput.branch(actions);
		}
	}

	public static void readEverythingFromBTree(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
		c.setParamInt(ReadFromBtree.PARALLEL_TASKS, nl.vu.cs.querypie.reasoner.common.Consts.READ_NUM_THREADS);
		c.setParamWritable(ReadFromBtree.TUPLE, new Query(new TLong(-1), new TLong(-1), new TLong(-1)));
		actions.add(c);
	}

	public static void readFakeTuple(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
		a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
		actions.add(a);
	}

	public static void reconnectAfter(int reconnectAfter, ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, reconnectAfter);
		actions.add(c);
	}

	public static void reloadPrecomputationOnRules(Collection<Rule> rules, ActionContext context, boolean incrementalFlag, boolean allRules) {
		for (Rule r : rules) {
			r.reloadPrecomputation(ReasoningContext.getInstance(), context, incrementalFlag, allRules);
		}
	}

	public static void removeDuplicates(ActionSequence actions) throws ActionNotConfiguredException {
		actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));
	}

	static void sort(ActionSequence actions, boolean additionalStepCounter) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, nl.vu.cs.querypie.reasoner.common.Consts.SORT_NUM_THREADS);
		if (additionalStepCounter) {
			c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName(), TInt.class.getName());
		} else {
			c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName());
		}
		c.setParamBoolean(PartitionToNodes.B_SORT, true);
		actions.add(c);
	}

	public static void writeInMemoryTuplesToBTree(int step, ActionContext context, ActionOutput actionOutput, String inMemoryKey) throws Exception {
		ActionSequence actions = new ActionSequence();
		readFakeTuple(actions);
		ReadAllInMemoryTriples.addToChain(actions, inMemoryKey);
		WriteDerivationsBtree.addToChain(step, actions);
		actionOutput.branch(actions);
	}
}
