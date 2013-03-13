package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.support.WritableListActions;
import nl.vu.cs.ajira.buckets.TupleSerializer;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
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
		c.setParamWritable(
				ReadFromBtree.TUPLE,
				new TupleSerializer(TupleFactory.newTuple(new TLong(-1),
						new TLong(-1), new TLong(-1))));
		actions.add(c);

		// Forward the input to the Map action
		c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.RECONNECT_AFTER_ACTIONS, 3);

		// TODO: First apply the rules that do not require any join

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

		// Map
		actions.add(ActionFactory.getActionConf(RuleExecutor1.class));

		// Group by
		c = ActionFactory.getActionConf(GroupBy.class);
		byte[] grouping_fields = new byte[r.getSharedVariablesGen_Head().length];
		for (byte i = 0; i < grouping_fields.length; ++i)
			grouping_fields[i] = i;
		c.setParamByteArray(GroupBy.FIELDS_TO_GROUP, 0);
		int lengthTuple = grouping_fields.length
				+ r.getSharedVariablesGen_Precomp().length;
		String[] fields = new String[lengthTuple];
		for (int i = 0; i < lengthTuple; ++i) {
			fields[i] = TLong.class.getName();
		}
		c.setParamStringArray(GroupBy.TUPLE_FIELDS, fields);
		c.setParamInt(GroupBy.NPARTITIONS_PER_NODE, 4);
		actions.add(c);

		// Reduce
		actions.add(ActionFactory.getActionConf(RuleExecutor2.class));
	}

	private void applyRulesSchemaOnly(List<ActionConf> actions) {

		// Read a fake triple in input
		ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamInt(QueryInputLayer.INPUT_LAYER, Consts.DUMMY_INPUT_LAYER_ID);
		a.setParamWritable(QueryInputLayer.TUPLE, new TupleSerializer(
				TupleFactory.newTuple()));
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

			// Add the controller
			ActionConf c = ActionFactory.getActionConf(RulesController.class);
			actions.add(c);

			// Create the branch
			actionOutput.branch(actions);

		}
	}
}
