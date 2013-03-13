package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Term;
import nl.vu.cs.querypie.reasoner.support.sets.Tuples;

public class PrecomputedRuleExecutor extends Action {

	public static final int RULE_ID = 0;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(RULE_ID, "rule", null, true);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		Rule rule = ReasoningContext.getInstance().getRuleset()
				.getAllSchemaOnlyRules()[getParamInt(RULE_ID)];

		List<SimpleData[]> results = new ArrayList<SimpleData[]>();
		for (int i = 0; i < 3; i++) {
			Term term = rule.getHead().getTerm(i);
			// The value is constant
			if (term.getName() != null) {
				TLong value = new TLong(term.getValue());
				if (results.isEmpty()) {
					SimpleData[] result = new SimpleData[3];
					result[i] = value;
					results.add(result);
				} else {
					for (SimpleData[] result : results) {
						result[i] = value;
					}
				}
			}
			// The value derives from a variable
			else {
				Tuples tuples = rule.getPrecomputedTuples();
				if (results.isEmpty()) {
					for (Long value : tuples.getSortedSet(i)) {
						SimpleData[] result = new SimpleData[3];
						result[i] = new TLong(value);
						results.add(result);
					}
				} else {
					List<SimpleData[]> newResults = new ArrayList<SimpleData[]>();
					for (SimpleData[] result : results) {
						for (Long value : tuples.getSortedSet(i)) {
							SimpleData[] newResult = new SimpleData[3];
							for (int j = 0; j < i; j++) {
								newResult[j] = result[j];
							}
							newResult[i] = new TLong(value);
							newResults.add(newResult);
						}
					}
					results = newResults;
					newResults = null;
				}
			}
		}

		for (SimpleData[] result : results) {
			actionOutput.output(result);
		}
	}

}
