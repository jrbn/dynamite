package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Utils;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;

class GenericRuleExecutor extends Action {

  List<int[][]> positions_gen_head = new ArrayList<int[][]>();
  List<SimpleData[]> outputTriples = new ArrayList<SimpleData[]>();
  private int[][] pos_constants_to_check;
  private long[][] value_constants_to_check;
  int[] counters;
  List<Rule> rules;

  @Override
  public void startProcess(ActionContext context) throws Exception {
    rules = ReasoningContext.getInstance().getRuleset().getAllRulesWithOneAntecedent();
    counters = new int[rules.size()];
    pos_constants_to_check = new int[rules.size()][];
    value_constants_to_check = new long[rules.size()][];
    for (int r = 0; r < rules.size(); ++r) {
      Rule rule = rules.get(r);
      // Determines positions of variables
      int[][] pos_gen_head = rule.getSharedVariablesGen_Head();
      positions_gen_head.add(pos_gen_head);
      // Determines the positions and values of constants
      pos_constants_to_check[r] = rule.getPositionsConstantGenericPattern();
      value_constants_to_check[r] = rule.getValueConstantGenericPattern();
      // Prepares the known parts of the output triples
      Pattern head = rule.getHead();
      SimpleData[] outputTriple = new SimpleData[3];
      for (int i = 0; i < 3; i++) {
        Term t = head.getTerm(i);
        if (t.getName() == null) {
          outputTriple[i] = new TLong(t.getValue());
        }
      }
      outputTriples.add(outputTriple);
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    // Bind the variables in the output triple
    for (int r = 0; r < outputTriples.size(); r++) {
      // Does the input match with the generic pattern?
      if (!Utils.tupleMatchConstants(tuple, pos_constants_to_check[r], value_constants_to_check[r])) {
        continue;
      }
      int[][] pos_gen_head = positions_gen_head.get(r);
      SimpleData[] outputTriple = outputTriples.get(r);
      for (int i = 0; i < pos_gen_head.length; ++i) {
        outputTriple[pos_gen_head[i][1]] = tuple.get(pos_gen_head[i][0]);
      }
      actionOutput.output(outputTriple);
      counters[r]++;
    }
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    for (int i = 0; i < counters.length; ++i) {
      context.incrCounter("derivation-rule-" + rules.get(i).getId(), counters[i]);
    }
  }

}
