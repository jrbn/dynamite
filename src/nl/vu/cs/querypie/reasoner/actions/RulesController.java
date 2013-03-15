package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
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
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.ReasoningContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesController extends Action {

	static final Logger log = LoggerFactory.getLogger(RulesController.class);

	private boolean hasDerived;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		hasDerived = false;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		hasDerived = true;
	}

	private void applyRulesWithGenericPatterns(List<ActionConf> actions) {

		// Read everything from the knowledge base
		ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
		c.setParamInt(ReadFromBtree.PARALLEL_TASKS, 4);
		c.setParamWritable(ReadFromBtree.TUPLE, new Query(new TLong(-1),
				new TLong(-1), new TLong(-1)));
		actions.add(c);

		// Forward the input to the Map action
		c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 2);
		actions.add(c);

		// First apply only the rules that use one antecedent
		c = ActionFactory.getActionConf(GenericRuleExecutor.class);
		actions.add(c);

		// Forward derivation to sorting phase
		c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 4);
		actions.add(c);

		// // Sort the derivation
		// c = ActionFactory.getActionConf(PartitionToNodes.class);
		// c.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE, 4);
		// c.setParamStringArray(PartitionToNodes.TUPLE_FIELDS,
		// TLong.class.getName(), TLong.class.getName(),
		// TLong.class.getName());
		// c.setParamBoolean(PartitionToNodes.SORT, true);
		// actions.add(c);
		//
		// // Remove the duplicates
		// actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));

		// // Write the derivation on the BTree
		// actions.add(ActionFactory.getActionConf(WriteDerivationsBtree.class));

		// Map
		actions.add(ActionFactory.getActionConf(PrecompGenericMap.class));

		// Group by
		c = ActionFactory.getActionConf(GroupBy.class);
		c.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
		c.setParamStringArray(GroupBy.TUPLE_FIELDS, TByteArray.class.getName(),
				TByte.class.getName(), TLong.class.getName());
		c.setParamInt(GroupBy.NPARTITIONS_PER_NODE, 4);
		actions.add(c);

		// Reduce
		actions.add(ActionFactory.getActionConf(PrecompGenericReduce.class));

		// Sort the derivation
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE, 4);
		c.setParamStringArray(PartitionToNodes.TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TLong.class.getName());
		c.setParamBoolean(PartitionToNodes.SORT, true);
		actions.add(c);

		// Remove the duplicates
		actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));

		// Write the derivation on the BTree
		actions.add(ActionFactory.getActionConf(WriteDerivationsBtree.class));
	}

	private void applyRulesSchemaOnly(List<ActionConf> actions) {

		// Read a fake triple in input
		ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
		a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
		actions.add(a);

		// Launch the schema-only rules in parallel
		a = ActionFactory.getActionConf(ParallelExecutionSchemaOnly.class);
		actions.add(a);

		// Sort the derivation
		a = ActionFactory.getActionConf(PartitionToNodes.class);
		a.setParamBoolean(PartitionToNodes.SORT, true);
		a.setParamStringArray(PartitionToNodes.TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TLong.class.getName());
		a.setParamInt(GroupBy.NPARTITIONS_PER_NODE, 4);
		actions.add(a);

		// Clean the duplicates
		a = ActionFactory.getActionConf(RemoveDuplicates.class);
		actions.add(a);

		// Write the derivation on the BTree
		a = ActionFactory.getActionConf(WriteDerivationsBtree.class);
		actions.add(a);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {

		// Launch only-schema rules
		if (hasDerived) {

			List<ActionConf> actions = new ArrayList<ActionConf>();

			if (ReasoningContext.getInstance().getRuleset()
					.getAllSchemaOnlyRules() != null) {
				applyRulesSchemaOnly(actions);

				// Reload the schema
				actions.add(ActionFactory.getActionConf(ReloadSchema.class));

				// Create a branch to process the rules that use generic
				// patterns
				List<ActionConf> actions2 = new ArrayList<ActionConf>();
				applyRulesWithGenericPatterns(actions2);
				ActionConf c = ActionFactory.getActionConf(Branch.class);
				c.setParamWritable(Branch.BRANCH, new WritableListActions(
						actions2));
				actions.add(c);
			} else {
				// There is no rule only on schema triples.
				applyRulesWithGenericPatterns(actions);
			}

			// Collect the derivations flags on one node
			ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.TUPLE_FIELDS,
					TLong.class.getName(), TLong.class.getName(),
					TLong.class.getName());
			actions.add(c);

			// Add the controller
			c = ActionFactory.getActionConf(RulesController.class);
			actions.add(c);

			// Create the branch
			actionOutput.branch(actions);

		}
	}
}
