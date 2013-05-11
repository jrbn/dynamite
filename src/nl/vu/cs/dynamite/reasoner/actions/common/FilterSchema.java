package nl.vu.cs.querypie.reasoner.actions.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Utils;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;

public class FilterSchema extends Action {

	int[][] pos_constants;
	long[][] value_constants;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		Map<Pattern, Collection<Rule>> patterns = ReasoningContext
				.getInstance().getRuleset().getPrecomputedPatternSet();

		Collection<Pattern> col = patterns.keySet();
		pos_constants = new int[col.size()][];
		value_constants = new long[col.size()][];
		int s = 0;
		for (Pattern pattern : col) {
			ArrayList<Integer> pos = new ArrayList<Integer>();
			ArrayList<Long> values = new ArrayList<Long>();
			for (int i = 0; i < 3; ++i) {
				Term t = pattern.getTerm(i);
				if (t.getValue() >= 0) {
					pos.add(i);
					values.add(t.getValue());
				}
			}
			pos_constants[s] = new int[pos.size()];
			value_constants[s] = new long[values.size()];
			for (int j = 0; j < pos.size(); ++j) {
				pos_constants[s][j] = pos.get(j);
				value_constants[s][j] = values.get(j);
			}
			s++;
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

		for (int i = 0; i < pos_constants.length; ++i) {
			if (Utils.tupleMatchConstants(tuple, pos_constants[i],
					value_constants[i])) {
				actionOutput.output(tuple);
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		pos_constants = null;
		value_constants = null;
	}
}
