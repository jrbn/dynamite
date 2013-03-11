package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.GroupBy;
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
  private static final int NUMBER_NOT_DERIVED = 1;

  private boolean hasDerived;
  private int lastExecutedRule;
  private int notDerivation;

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(LAST_EXECUTED_RULE, "rule", -1, false);
    conf.registerParameter(NUMBER_NOT_DERIVED, "rules that did not derive anything", 0, false);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    hasDerived = false;
    lastExecutedRule = getParamInt(LAST_EXECUTED_RULE);
    notDerivation = getParamInt(NUMBER_NOT_DERIVED);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    hasDerived = true;
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {

    Rule[] rules = ReasoningContext.getInstance().getRuleset();
    if (rules.length == 0) {
      log.warn("No rule to execute!");
      return;
    }

    if (!hasDerived) {
      notDerivation++;
    } else {
      notDerivation = 0;
    }

    if (notDerivation != rules.length) {
      // Continue applying the rules...

      lastExecutedRule++;
      if (lastExecutedRule == rules.length) {
        lastExecutedRule = 0;
      }

      // Set up the rule
      Rule r = rules[lastExecutedRule];
      r.reloadPrecomputation(ReasoningContext.getInstance(), context);

      List<ActionConf> actions = new ArrayList<ActionConf>();
      if (r.getGenericBodyPatterns().length == 0) {
        ActionConf c = ActionFactory.getActionConf(PrecomputedRuleExecutor.class);
        c.setParamInt(PrecomputedRuleExecutor.RULE_ID, r.getId());
        actions.add(c);
      } else if (r.getPrecomputedBodyPatterns().length == 0) {
        ActionConf c = ActionFactory.getActionConf(GenericRuleExecutor.class);
        c.setParamInt(GenericRuleExecutor.RULE_ID, r.getId());
        actions.add(c);
      } else {
        // Read the input
        ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
        c.setParamWritable(ReadFromBtree.TUPLE, getTuple(r.getGenericBodyPatterns()[0]));
        c.setParamInt(ReadFromBtree.PARALLEL_TASKS, 1);
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
        int lengthTuple = grouping_fields.length + r.getSharedVariablesGen_Precomp().length;
        String[] fields = new String[lengthTuple];
        for (int i = 0; i < lengthTuple; ++i) {
          fields[i] = TLong.class.getName();
        }
        c.setParamStringArray(GroupBy.TUPLE_FIELDS, fields);
        c.setParamInt(GroupBy.NPARTITIONS_PER_NODE, 4);
        actions.add(c);

        // Reduce
        c = ActionFactory.getActionConf(RuleExecutor2.class);
        c.setParamInt(RuleExecutor2.RULE_ID, r.getId());
        actions.add(c);

        // // Sort the derivation to be inserted in the B-Tree
        // c = ActionFactory.getActionConf(PartitionToNodes.class);
        // c.setParamBoolean(PartitionToNodes.SORT, true);
        // c.setParamInt(PartitionToNodes.NPARTITIONS_PER_NODE, 4);
        // c.setParamStringArray(PartitionToNodes.TUPLE_FIELDS,
        // TLong.class.getName(), TLong.class.getName(),
        // TLong.class.getName());
        // actions.add(c);
        //
        // // Remove possible duplicates
        // c = ActionFactory.getActionConf(RemoveDuplicates.class);
        // actions.add(c);

        // Add the triples to one index (and verify they do not
        // already exist)
        c = ActionFactory.getActionConf(WriteDerivationsBtree.class);
        actions.add(c);

        // Collect the results to one node
        c = ActionFactory.getActionConf(CollectToNode.class);
        c.setParamStringArray(CollectToNode.TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName());
        actions.add(c);

        // Controller
        c = ActionFactory.getActionConf(RulesController.class);
        c.setParamInt(LAST_EXECUTED_RULE, lastExecutedRule);
        c.setParamInt(NUMBER_NOT_DERIVED, notDerivation);
        actions.add(c);
      }

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
