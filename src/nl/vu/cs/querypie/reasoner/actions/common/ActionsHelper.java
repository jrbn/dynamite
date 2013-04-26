package nl.vu.cs.querypie.reasoner.actions.common;

import java.io.FilenameFilter;
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
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.WriteToFiles;
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
import nl.vu.cs.querypie.reasoner.actions.io.WriteDerivationsAllBtree;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecompGenericMap;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecompGenericReduce;
import nl.vu.cs.querypie.reasoner.actions.rules.PrecomputedRuleExecutor;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.TripleFileStorage;

public class ActionsHelper {

	public static void collectToNode(boolean includingCount,
			ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		if (includingCount) {
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					TLong.class.getName(), TLong.class.getName(),
					TLong.class.getName(), TInt.class.getName(),
					TInt.class.getName());
		} else {
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					TLong.class.getName(), TLong.class.getName(),
					TLong.class.getName(), TInt.class.getName());
		}
		actions.add(c);
	}

	public static void createBranch(ActionSequence actions,
			ActionSequence actionsToBranch) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(Branch.class);
		c.setParamWritable(Branch.W_BRANCH, actionsToBranch);
		actions.add(c);
	}

	private static void groupBy(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(GroupBy.class);
		c.setParamByteArray(GroupBy.BA_FIELDS_TO_GROUP, (byte) 0);
		c.setParamStringArray(GroupBy.SA_TUPLE_FIELDS,
				TByteArray.class.getName(), TBoolean.class.getName(),
				TByte.class.getName(), TLong.class.getName());
		c.setParamInt(GroupBy.I_NPARTITIONS_PER_NODE,
				nl.vu.cs.querypie.reasoner.support.Consts.GROUP_BY_NUM_THREADS);
		actions.add(c);
	}

	public static void mapReduce(int minimumStep, int outputStep,
			boolean incrementalFlag, ActionSequence actions)
			throws ActionNotConfiguredException {
		PrecompGenericMap.addToChain(minimumStep, incrementalFlag, actions);
		groupBy(actions);
		PrecompGenericReduce.addToChain(minimumStep, outputStep,
				incrementalFlag, actions);
	}

	static void executeAllSchemaRulesInParallel(int minimumStep,
			int outputStep, boolean incrementalFlag, ActionOutput actionOutput)
			throws Exception {
		Set<Integer> schemaOnlyRules = new HashSet<Integer>();
		for (int i = 0; i < ReasoningContext.getInstance().getRuleset()
				.getAllSchemaOnlyRules().size(); ++i) {
			schemaOnlyRules.add(i);
		}
		executeSchemaRulesInParallel(schemaOnlyRules, minimumStep, outputStep,
				incrementalFlag, actionOutput);
	}

	public static void executeSchemaRulesInParallel(Set<Integer> ruleIds,
			int minimumStep, int outputStep, boolean incrementalFlag,
			ActionOutput actionOutput) throws Exception {
		for (Integer ruleId : ruleIds) {
			ActionSequence actions = new ActionSequence();
			readFakeTuple(actions);
			PrecomputedRuleExecutor.addToChain(minimumStep, outputStep, ruleId,
					actions, incrementalFlag);
			actionOutput.branch(actions);
		}
	}

	public static void readEverythingFromBTree(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
		c.setParamInt(ReadFromBtree.I_PARALLEL_TASKS,
				nl.vu.cs.querypie.reasoner.support.Consts.READ_NUM_THREADS);
		c.setParamWritable(ReadFromBtree.W_TUPLE, new Query(new TLong(-1),
				new TLong(-1), new TLong(-1)));
		actions.add(c);
	}

	public static void readEverythingFromFiles(String copyDir,
			ActionSequence actions, Class<? extends FilenameFilter> filter)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_PATH, copyDir);
		c.setParamString(ReadFromFiles.S_CUSTOM_READER,
				TripleFileStorage.Reader.class.getName());
		if (filter != null) {
			c.setParamString(ReadFromFiles.S_FILE_FILTER, filter.getName());
		}
		actions.add(c);
	}

	public static void readFakeTuple(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamString(QueryInputLayer.S_INPUTLAYER,
				DummyLayer.class.getName());
		a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
		actions.add(a);
	}

	public static void reconnectAfter(int reconnectAfter, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, reconnectAfter);
		actions.add(c);
	}

	public static void reloadPrecomputationOnRules(Collection<Rule> rules,
			ActionContext context, boolean incrementalFlag, boolean allRules) {
		for (Rule r : rules) {
			r.reloadPrecomputation(ReasoningContext.getInstance(), context,
					incrementalFlag, allRules);
		}
	}

	public static void removeDuplicates(ActionSequence actions)
			throws ActionNotConfiguredException {
		actions.add(ActionFactory.getActionConf(TriplesRemoveDuplicates.class));
	}

	public static void sort(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE,
				nl.vu.cs.querypie.reasoner.support.Consts.SORT_NUM_THREADS);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TLong.class.getName(), TInt.class.getName());
		c.setParamBoolean(PartitionToNodes.B_SORT, true);
		c.setParamByteArray(PartitionToNodes.BA_PARTITION_FIELDS, (byte) 0,
				(byte) 1, (byte) 2);
		actions.add(c);
	}

	public static void writeInMemoryTuplesToBTree(ActionContext context,
			ActionOutput actionOutput, String inMemoryKey) throws Exception {
		ActionSequence actions = new ActionSequence();
		readFakeTuple(actions);
		ReadAllInMemoryTriples.addToChain(inMemoryKey, actions);
		WriteDerivationsAllBtree.addToChain(actions);
		actionOutput.branch(actions);
	}

	public static void writeCopyToFiles(String dir, ActionSequence actions,
			boolean counts) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamString(WriteToFiles.S_PATH, dir);
		if (!counts) {
			c.setParamString(WriteToFiles.S_CUSTOM_WRITER,
					TripleFileStorage.Writer.class.getName());
		} else {
			c.setParamString(WriteToFiles.S_CUSTOM_WRITER,
					TripleFileStorage.WriterCount.class.getName());
			c.setParamString(WriteToFiles.S_PREFIX_FILE, "_count");
		}
		actions.add(c);
	}

	public static void forwardOnlyFirst(ActionSequence actions)
			throws ActionNotConfiguredException {
		actions.add(ActionFactory.getActionConf(ForwardOnlyFirst.class));
	}

	public static void filterPotentialInput(int reconnectAt,
			ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, reconnectAt);
		actions.add(c);
	}

	public static void writeSchemaTriplesInBtree(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionSequence seq = new ActionSequence();
		seq.add(ActionFactory.getActionConf(FilterSchema.class));
		WriteDerivationsAllBtree.addToChain(seq);

		ActionConf c = ActionFactory.getActionConf(Split.class);
		c.setParamWritable(Split.W_SPLIT, seq);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 1);
		actions.add(c);
	}

	public static void filterStep(ActionSequence actions, int step)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(FilterStep.class);
		c.setParamInt(FilterStep.I_STEP, step);
		actions.add(c);
	}
}
