package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.buckets.TupleSerializer;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.Term;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesController extends Action {

	static final Logger log = LoggerFactory.getLogger(RulesController.class);
	public static final int LAST_EXECUTED_RULE = 0;

	private boolean hasDerived;
	private int lastExecutedRule;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(LAST_EXECUTED_RULE, "rule", -1, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		hasDerived = false;
		lastExecutedRule = getParamInt(LAST_EXECUTED_RULE);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		hasDerived = true;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (hasDerived) {
			// Continue applying the rules...

			Rule[] rules = ReasoningContext.getInstance().getRuleset();
			if (rules.length == 0) {
				log.warn("No rule to execute!");
				return;
			}

			lastExecutedRule++;
			if (lastExecutedRule < rules.length) {
				lastExecutedRule = 0;
			}

			Rule r = rules[lastExecutedRule];
			r.reloadPrecomputation(ReasoningContext.getInstance(), context);

			List<ActionConf> actions = new ArrayList<ActionConf>();
			ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
			c.setParamWritable(QueryInputLayer.TUPLE,
					getTuple(r.getGenericBodyPatterns()[0]));
			actions.add(c);

			// Map
			c = ActionFactory.getActionConf(RuleExecutor1.class);
			c.setParamInt(RuleExecutor1.RULE_ID, r.getId());
			actions.add(c);

			// Group by
			c = ActionFactory.getActionConf(GroupBy.class);
			byte[] grouping_fields = new byte[r.getSharedVariablesGen_Head().length];
			for (byte i = 0; i < grouping_fields.length; ++i)
				grouping_fields[i] = i;
			c.setParamByteArray(GroupBy.FIELDS_TO_GROUP, grouping_fields);
			int lengthTuple = grouping_fields.length
					+ r.getSharedVariablesGen_Precomp().length;
			String[] fields = new String[lengthTuple];
			for (int i = 0; i < lengthTuple; ++i) {
				fields[i] = TLong.class.getName();
			}
			c.setParamStringArray(GroupBy.TUPLE_FIELDS, fields);
			actions.add(c);

			// Reduce
			c = ActionFactory.getActionConf(RuleExecutor2.class);
			c.setParamInt(RuleExecutor2.RULE_ID, r.getId());
			actions.add(c);

			// Sort the derivation to be inserted in the B-Tree
			c = ActionFactory.getActionConf(PartitionToNodes.class);
			c.setParamBoolean(PartitionToNodes.SORT, true);
			c.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE, 6);
			actions.add(c);

			// Remove possible duplicates
			c = ActionFactory.getActionConf(RemoveDuplicates.class);
			actions.add(c);

			// Add the triples to one index (and verify they do not
			// already exist)

			// TODO: Add only the new ones to the other indices

			// Controller
			c = ActionFactory.getActionConf(RulesController.class);
			c.setParamInt(LAST_EXECUTED_RULE, lastExecutedRule);
			actions.add(c);

			// Execute the rule
			actionOutput.branch(actions);
		}
	}

	private TupleSerializer getTuple(Pattern pattern) {
		TLong[] t = { new TLong(), new TLong(), new TLong() };
		for (int i = 0; i < 3; ++i) {
			Term term = pattern.getTerm(i);
			if (term.getName() == null) {
				t[i].setValue(term.getValue());
			} else {
				t[i].setValue(-1);
			}
		}

		return new TupleSerializer(TupleFactory.newTuple(t));
	}
}
