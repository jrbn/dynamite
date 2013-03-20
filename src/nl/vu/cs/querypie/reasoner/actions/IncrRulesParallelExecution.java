package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;
import nl.vu.cs.querypie.storage.inmemory.Tuples;

public class IncrRulesParallelExecution extends Action {

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {

		// Find out whether there is schema information in the delta
		InMemoryTupleSet set = (InMemoryTupleSet) context
				.getObjectFromCache("delta");

		Map<Pattern, Collection<Rule>> patterns = ReasoningContext
				.getInstance().getRuleset().getPrecomputedPatternSet();
		Iterator<Pattern> itr = patterns.keySet().iterator();
		while (itr.hasNext()) {
			Pattern p = itr.next();
			Set<Tuple> tuples = set.getSubset(p);
			if (tuples == null || tuples.size() == 0) {
				itr.remove();
			}
		}

		// "patterns" contains all the patterns which have triples in delta. Get
		// all the rules that use them
		Set<Rule> rulesToLaunch = new HashSet<Rule>();
		for (Map.Entry<Pattern, Collection<Rule>> entry : patterns.entrySet()) {
			rulesToLaunch.addAll(entry.getValue());
		}
		List<Rule> rulesOnlySchema = new ArrayList<Rule>();
		List<Rule> rulesSchemaGenerics = new ArrayList<Rule>();
		for (Rule rule : rulesToLaunch) {
			if (rule.getGenericBodyPatterns() == null
					|| rule.getGenericBodyPatterns().length == 0) {
				rulesOnlySchema.add(rule);
			} else {
				rulesSchemaGenerics.add(rule);
			}
		}

		// Create n branches, one per every schema rule to be launched
		for (int i = 0; i < rulesOnlySchema.size(); ++i) {
			List<ActionConf> actions = new ArrayList<ActionConf>();

			ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
			a.setParamInt(QueryInputLayer.I_INPUTLAYER,
					Consts.DUMMY_INPUT_LAYER_ID);
			a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
			actions.add(a);

			a = ActionFactory.getActionConf(PrecomputedRuleExecutor.class);
			a.setParamInt(PrecomputedRuleExecutor.RULE_ID, i);
			a.setParamBoolean(PrecomputedRuleExecutor.INCREMENTAL_FLAG, true);
			actions.add(a);

			actionOutput.branch(actions);
		}

		/******
		 * Read all the delta triples and apply on them all the rules with a
		 * single antecedent.
		 ******/
		List<ActionConf> actions = new ArrayList<ActionConf>();
		ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
		a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
		actions.add(a);

		// Read all the triples from the delta and stream them to the rest of
		// the chain
		actions.add(ActionFactory.getActionConf(ReadAllInmemoryTriples.class));

		// Apply all the rules on them
		a = ActionFactory.getActionConf(GenericRuleExecutor.class);
		actions.add(a);

		actionOutput.branch(actions);

		/*****
		 * Apply the rules that require a map and reduce.
		 */
		actions = new ArrayList<ActionConf>();
		a = ActionFactory.getActionConf(QueryInputLayer.class);
		a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
		a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
		actions.add(a);

		// Read all the triples from the delta and stream them to the rest of
		// the chain
		actions.add(ActionFactory.getActionConf(ReadAllInmemoryTriples.class));

		// Map
		a = ActionFactory.getActionConf(PrecompGenericMap.class);
		actions.add(a);

		// Group by
		a = ActionFactory.getActionConf(GroupBy.class);
		a.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
		a.setParamStringArray(GroupBy.TUPLE_FIELDS, TByteArray.class.getName(),
				TByte.class.getName(), TLong.class.getName());
		actions.add(a);

		// Reduce
		a = ActionFactory.getActionConf(PrecompGenericReduce.class);
		actions.add(a);

		actionOutput.branch(actions);

		/****
		 * If some schema is changed, reapply the rules over the entire input
		 * which is affected
		 */
		for (Rule r : rulesSchemaGenerics) {
			// Get all the possible "join" values that match the schema
			Pattern query = new Pattern();
			r.getGenericBodyPatterns()[0].copyTo(query);

			Tuples tuples = r.getFlaggedPrecomputedTuples();
			int[][] shared_pos = r.getSharedVariablesGen_Precomp();
			Collection<Long> possibleValues = tuples
					.getSortedSet(shared_pos[0][1]);
			for (long v : possibleValues) {
				query.setTerm(shared_pos[0][0], new Term(v));
				actions = new ArrayList<ActionConf>();

				// Read the query from the btree
				a = ActionFactory.getActionConf(ReadFromBtree.class);
				a.setParamWritable(ReadFromBtree.TUPLE, new Query(new TLong(
						query.getTerm(0).getValue()), new TLong(query
						.getTerm(1).getValue()), new TLong(query.getTerm(2)
						.getValue())));
				actions.add(a);

				// Map
				a = ActionFactory.getActionConf(PrecompGenericMap.class);
				a.setParamBoolean(PrecompGenericMap.INCREMENTAL_FLAG, true);
				actions.add(a);

				// Group by
				a = ActionFactory.getActionConf(GroupBy.class);
				a.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
				a.setParamStringArray(GroupBy.TUPLE_FIELDS,
						TByteArray.class.getName(), TByte.class.getName(),
						TLong.class.getName());
				actions.add(a);

				// Reduce
				a = ActionFactory.getActionConf(PrecompGenericReduce.class);
				a.setParamBoolean(PrecompGenericReduce.INCREMENTAL_FLAG, true);
				actions.add(a);

				actionOutput.branch(actions);
			}
		}

	}
}
