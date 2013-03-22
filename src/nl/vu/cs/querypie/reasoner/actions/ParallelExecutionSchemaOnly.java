package nl.vu.cs.querypie.reasoner.actions;

import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ParallelExecutionSchemaOnly extends Action {

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    List<Rule> rules = ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules();
    ActionsHelper.parallelRunPrecomputedRuleExecutorForAllRules(rules.size(), false, actionOutput);
  }
}
