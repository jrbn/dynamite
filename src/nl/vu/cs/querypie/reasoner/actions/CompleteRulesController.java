package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;

/**
 * A rules controller that execute the complete materialization of all the tuples based on the facts written on the
 * knowledge base and on the derivation rules.
 */
public class CompleteRulesController extends AbstractRulesController {
  private boolean hasDerived;

  @Override
  public void startProcess(ActionContext context) throws Exception {
    hasDerived = false;
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    hasDerived = true;
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    if (!hasDerived) return;
    List<ActionConf> actions = new ArrayList<ActionConf>();
    if (!ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().isEmpty()) {
      applyRulesSchemaOnly(actions, true, false);
      applyRulesWithGenericPatternsInABranch(actions, true, false);
    } else {
      applyRulesWithGenericPatterns(actions, true, false);
    }
    ActionsHelper.collectToNode(actions);
    ActionsHelper.runCompleteRulesController(actions);
    actionOutput.branch(actions);
  }

}
