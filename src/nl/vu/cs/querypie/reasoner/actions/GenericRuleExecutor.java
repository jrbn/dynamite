package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.Term;

public class GenericRuleExecutor extends Action {

  public static final int RULE_ID = 0;

  private Rule rule;
  private int[][] pos_gen_head;

  private final SimpleData[] outputTriple = new SimpleData[3];

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(RULE_ID, "rule", null, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    rule = ReasoningContext.getInstance().getRule(getParamInt(RULE_ID));
    pos_gen_head = rule.getSharedVariablesGen_Head();
    Pattern head = rule.getHead();
    for (int i = 0; i < 2; i++) {
      Term t = head.getTerm(i);
      if (t.getName() == null) {
        outputTriple[i] = new TLong(t.getValue());
      }
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    // Copy the "key" in the output triple
    for (int i = 0; i < pos_gen_head.length; ++i) {
      outputTriple[pos_gen_head[i][1]] = tuple.get(i);
    }
    actionOutput.output(outputTriple);
    System.out.println(outputTriple[0] + " " + outputTriple[1] + " " + outputTriple[2]);
  }

}
