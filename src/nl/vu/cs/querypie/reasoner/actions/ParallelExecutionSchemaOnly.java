package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ParallelExecutionSchemaOnly extends Action {

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {

  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    // Create n branches, one per every rule
    Rule[] rules = ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules();
    for (int i = 0; i < rules.length; ++i) {
      List<ActionConf> actions = new ArrayList<ActionConf>();

      ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
      a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
      a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
      actions.add(a);

      a = ActionFactory.getActionConf(PrecomputedRuleExecutor.class);
      a.setParamInt(PrecomputedRuleExecutor.RULE_ID, i);
      a.setParamBoolean(PrecomputedRuleExecutor.INCREMENTAL_FLAG, false);
      actions.add(a);

      actionOutput.branch(actions);
    }
  }
}
