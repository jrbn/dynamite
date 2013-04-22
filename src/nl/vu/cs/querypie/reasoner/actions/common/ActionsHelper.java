package nl.vu.cs.querypie.reasoner.actions.common;

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
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.io.ReadAllInMemoryTriples;
import nl.vu.cs.querypie.reasoner.actions.io.ReadFromBtree;
import nl.vu.cs.querypie.reasoner.actions.io.WriteDerivationsBtree;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecompGenericMap;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecompGenericReduce;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecomputedRuleExecutor;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ActionsHelper {

	public static void collectToNode(boolean includingCount, ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		if (includingCount) {
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName(), TInt.class.getName(),
					TInt.class.getName());
		} else {
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName(), TInt.class.getName());
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
		c.setParamInt(GroupBy.I_NPARTITIONS_PER_NODE, nl.vu.cs.querypie.reasoner.support.Consts.GROUP_BY_NUM_THREADS);
		actions.add(c);
	}

	public static void mapReduce(int minimumStep, int outputStep, boolean incrementalFlag, ActionSequence actions) throws ActionNotConfiguredException {
		PrecompGenericMap.addToChain(minimumStep, incrementalFlag, actions);
		groupBy(actions);
		PrecompGenericReduce.addToChain(minimumStep, outputStep, incrementalFlag, actions);
	}

	static void executeAllSchemaRulesInParallel(int minimumStep, int outputStep, boolean incrementalFlag, ActionOutput actionOutput) throws Exception {
		Set<Integer> schemaOnlyRules = new HashSet<Integer>();
		for (int i = 0; i < ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().size(); ++i) {
			schemaOnlyRules.add(i);
		}
		executeSchemaRulesInParallel(schemaOnlyRules, minimumStep, outputStep, incrementalFlag, actionOutput);
	}

	public static void executeSchemaRulesInParallel(Set<Integer> ruleIds, int minimumStep, int outputStep, boolean incrementalFlag, ActionOutput actionOutput)
			throws Exception {
		for (Integer ruleId : ruleIds) {
			ActionSequence actions = new ActionSequence();
			readFakeTuple(actions);
			PrecomputedRuleExecutor.addToChain(minimumStep, outputStep, ruleId, actions, incrementalFlag);
			actionOutput.branch(actions);
		}
	}

	public static void readEverythingFromBTree(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
		c.setParamInt(ReadFromBtree.PARALLEL_TASKS, nl.vu.cs.querypie.reasoner.support.Consts.READ_NUM_THREADS);
		c.setParamWritable(ReadFromBtree.TUPLE, new Query(new TLong(-1), new TLong(-1), new TLong(-1)));
		actions.add(c);
	}

	public static void readFakeTuple(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamString(QueryInputLayer.S_INPUTLAYER, DummyLayer.class.getName());
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

	static void sort(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, nl.vu.cs.querypie.reasoner.support.Consts.SORT_NUM_THREADS);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName(), TInt.class.getName());
		c.setParamBoolean(PartitionToNodes.B_SORT, true);
		actions.add(c);
	}

	public static void writeInMemoryTuplesToBTree(ActionContext context, ActionOutput actionOutput, String inMemoryKey) throws Exception {
		ActionSequence actions = new ActionSequence();
		readFakeTuple(actions);
		ReadAllInMemoryTriples.addToChain(inMemoryKey, actions);
		WriteDerivationsBtree.addToChain(actions);
		actionOutput.branch(actions);
	}
}
