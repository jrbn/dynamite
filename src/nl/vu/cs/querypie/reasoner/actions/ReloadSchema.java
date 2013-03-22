package nl.vu.cs.querypie.reasoner.actions;

import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ReloadSchema extends Action {

  static final int INCREMENTAL_FLAG = 0;

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(INCREMENTAL_FLAG, "incremental_flag", false, true);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    actionOutput.output(tuple);
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    if (!context.isPrincipalBranch()) return;
    boolean incrementalFlag = getParamBoolean(INCREMENTAL_FLAG);
    List<Rule> rulesWithSchemaAndGeneric = ReasoningContext.getInstance().getRuleset().getAllRulesWithSchemaAndGeneric();
    ActionsHelper.reloadPrecomputationOnRules(rulesWithSchemaAndGeneric, context, incrementalFlag);
  }
}
