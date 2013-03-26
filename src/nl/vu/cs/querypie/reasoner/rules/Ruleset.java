package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.storage.Pattern;

public class Ruleset {

  private List<Rule> schemaOnly = null;
  private List<Rule> oneAntecedent = null;
  private List<Rule> schemaGeneric = null;

  public Ruleset(List<Rule> rules) {
    schemaOnly = new ArrayList<Rule>();
    oneAntecedent = new ArrayList<Rule>();
    schemaGeneric = new ArrayList<Rule>();
    for (Rule r : rules) {
      if (r.isSchemaOnly()) {
        schemaOnly.add(r);
      } else if (r.isGenericOnly()) {
        oneAntecedent.add(r);
      } else {
        schemaGeneric.add(r);
      }
    }
  }

  public void init(ReasoningContext reasoningContext) {
    for (Rule r : schemaOnly) {
      r.init(reasoningContext);
    }
    for (Rule r : oneAntecedent) {
      r.init(reasoningContext);
    }
    for (Rule r : schemaGeneric) {
      r.init(reasoningContext);
    }
  }

  public List<Rule> getAllSchemaOnlyRules() {
    return schemaOnly;
  }

  public List<Rule> getAllRulesWithOneAntecedent() {
    return oneAntecedent;
  }

  public List<Rule> getAllRulesWithSchemaAndGeneric() {
    return schemaGeneric;
  }

  public Map<Pattern, Collection<Rule>> getPrecomputedPatternSet() {
    Map<String, Collection<Rule>> output = new HashMap<String, Collection<Rule>>();
    if (schemaOnly != null) {
      for (Rule r : schemaOnly) {
        for (Pattern p : r.getPrecomputedBodyPatterns()) {
          String signature = p.toStringWithVarsAsStar();
          Collection<Rule> col = null;
          if (!output.containsKey(signature)) {
            col = new ArrayList<Rule>();
            output.put(signature, col);
          } else {
            col = output.get(signature);
          }
          col.add(r);
        }
      }
    }

    if (schemaGeneric != null) {
      for (Rule r : schemaGeneric) {
        for (Pattern p : r.getPrecomputedBodyPatterns()) {
          String signature = p.toStringWithVarsAsStar();
          Collection<Rule> col = null;
          if (!output.containsKey(signature)) {
            col = new ArrayList<Rule>();
            output.put(signature, col);
          } else {
            col = output.get(signature);
          }
          col.add(r);
        }
      }
    }

    // Translate the strings in patterns
    Map<Pattern, Collection<Rule>> output2 = new HashMap<Pattern, Collection<Rule>>();
    for (Map.Entry<String, Collection<Rule>> entry : output.entrySet()) {
      // Get the pattern from the first rule
      Rule r = entry.getValue().iterator().next();
      for (Pattern p : r.getPrecomputedBodyPatterns()) {
        if (p.toStringWithVarsAsStar().equals(entry.getKey())) {
          output2.put(p, entry.getValue());
          break;
        }
      }
    }
    return output2;
  }
}
