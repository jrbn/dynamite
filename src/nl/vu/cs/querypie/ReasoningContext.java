package nl.vu.cs.querypie;

import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ReasoningContext {

	private Rule[] ruleset;

	ReasoningContext() {
	}

	public void init(Rule[] ruleset) {
		this.ruleset = ruleset;
	}

	public Rule[] getRuleset() {
		return ruleset;
	}
}
