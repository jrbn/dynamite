package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.storage.Pattern;

public class Ruleset {

	private Rule[] schemaOnly = null;
	private Rule[] oneAntecedent = null;
	private Rule[] schemaGeneric = null;

	public Ruleset(Rule[] rules) {
		// For each rule determines which type it is
		List<Rule> schemaOnly = new ArrayList<Rule>();
		List<Rule> oneAntecedent = new ArrayList<Rule>();
		List<Rule> schemaGeneric = new ArrayList<Rule>();

		if (rules == null) {
			return;
		}

		for (Rule r : rules) {
			// Check if all the antecedents are "schema".
			if (r.getGenericBodyPatterns().length == 0) {
				schemaOnly.add(r);
				continue;
			}

			// If only one antecedent...
			if (r.getGenericBodyPatterns().length == 1
					&& r.getPrecomputedBodyPatterns().length == 0) {
				oneAntecedent.add(r);
				continue;
			}

			// All the others
			schemaGeneric.add(r);
		}

		// Reconvert them into arrays
		this.schemaOnly = schemaOnly.toArray(new Rule[schemaOnly.size()]);
		this.oneAntecedent = oneAntecedent.toArray(new Rule[oneAntecedent
				.size()]);
		this.schemaGeneric = schemaGeneric.toArray(new Rule[schemaGeneric
				.size()]);
	}

	public void init(ReasoningContext reasoningContext) {
		// Init all the rules
		if (schemaOnly != null) {
			for (Rule r : schemaOnly) {
				r.init(reasoningContext);
			}
		}

		if (oneAntecedent != null) {
			for (Rule r : oneAntecedent) {
				r.init(reasoningContext);
			}
		}

		if (schemaGeneric != null) {
			for (Rule r : schemaGeneric) {
				r.init(reasoningContext);
			}
		}
	}

	public Rule[] getAllSchemaOnlyRules() {
		return schemaOnly;
	}

	public Rule[] getAllRulesWithOneAntecedent() {
		return oneAntecedent;
	}

	public Rule[] getAllRulesWithSchemaAndGeneric() {
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
