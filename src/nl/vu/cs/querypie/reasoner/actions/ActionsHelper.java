package nl.vu.cs.querypie.reasoner.actions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.support.FilterHiddenFiles;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.actions.support.WritableListActions;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class ActionsHelper {

	static void createBranch(List<ActionConf> actions,
			List<ActionConf> actionsToBranch) {
		ActionConf c = ActionFactory.getActionConf(Branch.class);
		c.setParamWritable(Branch.BRANCH, new WritableListActions(
				actionsToBranch));
		actions.add(c);
	}

	static void runMapReduce(List<ActionConf> actions, int minimumStep,
			boolean incrementalFlag) {
		runMap(actions, minimumStep, incrementalFlag);
		runGroupBy(actions);
		runReduce(actions, minimumStep, incrementalFlag);
	}

	static void readEverythingFromBTree(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
		c.setParamInt(ReadFromBtree.PARALLEL_TASKS, 4);
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

	static void reconnectAfter(int reconnectAfter, List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, reconnectAfter);
		actions.add(c);
	}

	static void reloadPrecomputationOnRules(Collection<Rule> rules,
			ActionContext context, boolean incrementalFlag) {
		for (Rule r : rules) {
			r.reloadPrecomputation(ReasoningContext.getInstance(), context,
					incrementalFlag);
		}
	}

	static void runCollectToNode(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TLong.class.getName());
		actions.add(c);
	}

	static void runGenericRuleExecutor(int step, List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(GenericRuleExecutor.class);
		c.setParamInt(GenericRuleExecutor.I_MIN_STEP_TO_INCLUDE, step);
		c.setParamBoolean(GenericRuleExecutor.B_CHECK_VALID_INPUT, true);
		actions.add(c);
	}

	private static void runGroupBy(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(GroupBy.class);
		c.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
		c.setParamStringArray(GroupBy.TUPLE_FIELDS, TByteArray.class.getName(),
				TBoolean.class.getName(), TByte.class.getName(),
				TLong.class.getName());
		c.setParamInt(GroupBy.NPARTITIONS_PER_NODE, 4);
		actions.add(c);
	}

	public static void runIncrRulesController(List<ActionConf> actions,
			String deltaDir) {
		runIncrRulesControllerInStage(0, actions, deltaDir);
	}

	static void runIncrRulesControllerInStage(int stage,
			List<ActionConf> actions, String deltaDir) {
		ActionConf c = ActionFactory.getActionConf(IncrRulesController.class);
		c.setParamInt(IncrRulesController.I_STAGE, stage);
		c.setParamString(IncrRulesController.S_DELTA_DIR, deltaDir);
		actions.add(c);
	}

	static void runIncrRulesParallelExecution(List<ActionConf> actions) {
		actions.add(ActionFactory
				.getActionConf(IncrRulesParallelExecution.class));
	}

	private static void runMap(List<ActionConf> actions, int minimumStep,
			boolean incrementalFlag) {
		ActionConf c = ActionFactory.getActionConf(PrecompGenericMap.class);
		c.setParamBoolean(PrecompGenericMap.B_INCREMENTAL_FLAG, incrementalFlag);
		c.setParamInt(PrecompGenericMap.I_MINIMUM_STEP, minimumStep);
		actions.add(c);
	}

	static void parallelRunPrecomputedRuleExecutorForAllRules(int step,
			int numRules, boolean incrementalFlag, ActionOutput actionOutput)
			throws Exception {
		for (int ruleId = 0; ruleId < numRules; ++ruleId) {
			List<ActionConf> actions = new ArrayList<ActionConf>();
			readFakeTuple(actions);
			runPrecomputedRuleExecutorForRule(step, ruleId, actions,
					incrementalFlag);
			actionOutput.branch(actions);
		}
	}

	static void runPrecomputedRuleExecutorForRule(int step, int ruleId,
			List<ActionConf> actions, boolean incrementalFlag) {
		ActionConf a = ActionFactory
				.getActionConf(PrecomputedRuleExecutor.class);
		a.setParamInt(PrecomputedRuleExecutor.RULE_ID, ruleId);
		a.setParamBoolean(PrecomputedRuleExecutor.INCREMENTAL_FLAG,
				incrementalFlag);
		a.setParamInt(PrecomputedRuleExecutor.I_STEP, step);
		actions.add(a);
	}

	static void parallelRunPrecomputedRuleExecutorForRules(
			List<Integer> ruleIds, boolean incrementalFlag,
			ActionOutput actionOutput) throws Exception {
		for (Integer ruleId : ruleIds) {
			List<ActionConf> actions = new ArrayList<ActionConf>();
			readFakeTuple(actions);
			runPrecomputedRuleExecutorForRule(Integer.MIN_VALUE, ruleId,
					actions, incrementalFlag);
			actionOutput.branch(actions);
		}
	}

	static void runReadAllInMemoryTuples(List<ActionConf> actions) {
		actions.add(ActionFactory.getActionConf(ReadAllInmemoryTriples.class));
	}

	static void runReadFromBTree(Pattern pattern, List<ActionConf> actions) {
		ActionConf a = ActionFactory.getActionConf(ReadFromBtree.class);
		Query query = new Query(new TLong(pattern.getTerm(0).getValue()),
				new TLong(pattern.getTerm(1).getValue()), new TLong(pattern
						.getTerm(2).getValue()));
		a.setParamWritable(ReadFromBtree.TUPLE, query);
		actions.add(a);
	}

	private static void runReduce(List<ActionConf> actions, int minimumStep,
			boolean incrementalFlag) {
		ActionConf c = ActionFactory.getActionConf(PrecompGenericReduce.class);
		c.setParamBoolean(PrecompGenericReduce.INCREMENTAL_FLAG,
				incrementalFlag);
		c.setParamInt(PrecompGenericReduce.I_MINIMUM_STEP, minimumStep);
		actions.add(c);
	}

	static void runReloadSchema(List<ActionConf> actions,
			boolean incrementalFlag) {
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TLong.class.getName());
		actions.add(c);

		c = ActionFactory.getActionConf(ReloadSchema.class);
		c.setParamBoolean(ReloadSchema.INCREMENTAL_FLAG, incrementalFlag);
		actions.add(c);
	}

	static void addDerivationCount(List<ActionConf> actions, boolean groupSteps) {
		ActionConf c = ActionFactory.getActionConf(AddDerivationCount.class);
		c.setParamBoolean(AddDerivationCount.B_GROUP_STEPS, groupSteps);
		actions.add(c);
	}

	public static void runRulesController(int step, List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(RulesController.class);
		c.setParamInt(RulesController.I_STEP, step);
		actions.add(c);
	}

	static void runSchemaRulesInParallel(int step, List<ActionConf> actions) {
		ActionConf a = ActionFactory
				.getActionConf(ParallelExecutionSchemaOnly.class);
		a.setParamInt(ParallelExecutionSchemaOnly.I_STEP, step);
		actions.add(a);
	}

	static void runSort(List<ActionConf> actions, boolean additionalStepCounter) {
		ActionConf c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE, 4);
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

	static void runWriteDerivationsOnBTree(boolean forceStep, int step,
			List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(WriteDerivationsBtree.class);
		c.setParamInt(WriteDerivationsBtree.I_STEP, step);
		c.setParamBoolean(WriteDerivationsBtree.B_FORCE_STEP, forceStep);
		actions.add(c);
	}

	static TupleSet parseTriplesFromFile(String input) throws Exception {
		TupleSet set = new TupleSetImpl();
		List<File> files = new ArrayList<File>();
		File fInput = new File(input);
		if (fInput.isDirectory()) {
			for (File child : fInput.listFiles(new FilterHiddenFiles()))
				files.add(child);
		} else {
			files.add(fInput);
		}
		for (File file : files) {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				// Parse the line
				String[] sTriple = line.split(" ");
				TLong[] triple = { new TLong(), new TLong(), new TLong() };
				triple[0].setValue(Long.valueOf(sTriple[0]));
				triple[1].setValue(Long.valueOf(sTriple[1]));
				triple[2].setValue(Long.valueOf(sTriple[2]));
				set.add(TupleFactory.newTuple(triple));
			}
			reader.close();
		}
		return set;
	}

	public static void setStep(int step, List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(SetStep.class);
		c.setParamInt(SetStep.I_STEP, step);
		actions.add(c);
	}
}
