package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.Tuples;

public class PrecomputedRuleExecutor extends Action {

  public static final int RULE_ID = 0;
  public static final int INCREMENTAL_FLAG = 1;

  private Rule rule;
  private int counter;
  private boolean incrementalFlag;

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(RULE_ID, "rule", null, true);
    conf.registerParameter(INCREMENTAL_FLAG, "incremental_flag", false, true);
  }

  @Override
  public void startProcess(ActionContext context) {
    int ruleId = getParamInt(RULE_ID);
    incrementalFlag = getParamBoolean(INCREMENTAL_FLAG);
    rule = ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().get(ruleId);
    rule.reloadPrecomputation(ReasoningContext.getInstance(), context, incrementalFlag);
    counter = 0;
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    List<SimpleData[]> results = new ArrayList<SimpleData[]>();
    Map<Integer, Collection<Long>> values = new HashMap<Integer, Collection<Long>>();
    Map<Integer, Long> constants = new HashMap<Integer, Long>();

    int variableId = 0;
    for (int i = 0; i < 3; i++) {
      Term term = rule.getHead().getTerm(i);
      if (term.getName() == null) {
        constants.put(i, term.getValue());
      } else {
        Tuples tuples = incrementalFlag ? rule.getFlaggedPrecomputedTuples() : rule.getAllPrecomputedTuples();
        Collection<Long> sortedSet = tuples.getSortedSet(variableId++);
        values.put(i, sortedSet);
      }
    }

    int numResults = 0;
    for (Integer key : values.keySet()) {
      numResults = values.get(key).size();
      break;
    }

    for (int i = 0; i < numResults; i++) {
      SimpleData[] result = new SimpleData[3];
      results.add(result);
    }

    for (int i = 0; i < 3; i++) {
      if (values.containsKey(i)) {
        int num = 0;
        for (Long l : values.get(i)) {
          results.get(num)[i] = new TLong(l);
          num++;
        }
      } else {
        assert (constants.containsKey(i));
        Long val = constants.get(i);
        for (SimpleData[] result : results) {
          result[i] = new TLong(val);
        }
      }
    }

    for (SimpleData[] result : results) {
      actionOutput.output(result);
      counter++;
    }
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    context.incrCounter("derivation-rule-" + rule.getId(), counter);
  }

}
