package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.querypie.ReasoningContext;

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
			if (r.getGenericBodyPatterns() == null) {
				schemaOnly.add(r);
				continue;
			}

			// If only one antecedent...
			if (r.getGenericBodyPatterns().length == 1) {
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

}
