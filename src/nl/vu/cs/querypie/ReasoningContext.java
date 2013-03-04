package nl.vu.cs.querypie;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.schema.SchemaManager;

public class ReasoningContext {

	private static final ReasoningContext context = new ReasoningContext();

	private Rule[] ruleset;
	private Ajira arch;
	private SchemaManager manager;

	private ReasoningContext() {
	}

	public void init(Ajira arch, Rule[] ruleset) {
		this.arch = arch;
		this.ruleset = ruleset;
		manager = new SchemaManager(arch);
	}

	public Ajira getFramework() {
		return arch;
	}

	public Rule[] getRuleset() {
		return ruleset;
	}

	public SchemaManager getSchemaManager() {
		return manager;
	}

	public static ReasoningContext getInstance() {
		return context;
	}
}
