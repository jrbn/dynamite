package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.support.WritableListActions;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesController extends Action {

  static final Logger log = LoggerFactory.getLogger(RulesController.class);

  private boolean hasDerived;

  @Override
  public void startProcess(ActionContext context) throws Exception {
    hasDerived = false;
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    hasDerived = true;
  }

  private void applyRulesWithGenericPatterns(List<ActionConf> actions) {
    ActionsHelper.readEverythingFromBTree(actions);
    ActionsHelper.reconnectAfter(2, actions);
    ActionsHelper.runGenericRuleExecutor(actions);
    ActionsHelper.reconnectAfter(4, actions);
    ActionsHelper.runMap(actions, false);
    ActionsHelper.runGroupBy(actions);
    ActionsHelper.runReduce(actions, false);
    ActionsHelper.runSort(actions);
    ActionsHelper.runRemoveDuplicates(actions);
    ActionsHelper.runWriteDerivationsOnBTree(actions);
  }

  private void applyRulesSchemaOnly(List<ActionConf> actions) {
    ActionsHelper.readFakeTuple(actions);
    ActionsHelper.runSchemaRulesInParallel(actions);
    ActionsHelper.runSort(actions);
    ActionsHelper.runRemoveDuplicates(actions);
    ActionsHelper.runWriteDerivationsOnBTree(actions);
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    if (!hasDerived) return;
    List<ActionConf> actions = new ArrayList<ActionConf>();
    // Launch only-schema rules
    if (ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().length > 0) {
      applyRulesSchemaOnly(actions);
      ActionsHelper.runReloadSchema(actions, false);
      // Create a branch to process the rules that use generic patterns
      List<ActionConf> actions2 = new ArrayList<ActionConf>();
      applyRulesWithGenericPatterns(actions2);
      ActionConf c = ActionFactory.getActionConf(Branch.class);
      c.setParamWritable(Branch.BRANCH, new WritableListActions(actions2));
      actions.add(c);
    }
    // There is no rule only on schema triples
    else {
      applyRulesWithGenericPatterns(actions);
    }

    ActionsHelper.runCollectToNode(actions);
    ActionsHelper.runRulesController(actions);
    actionOutput.branch(actions);
  }

}
