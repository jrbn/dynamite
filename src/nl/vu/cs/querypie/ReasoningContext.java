package nl.vu.cs.querypie;

import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.querypie.reasoner.rules.Ruleset;
import nl.vu.cs.querypie.schema.SchemaManager;

public class ReasoningContext {

	private static final ReasoningContext context = new ReasoningContext();

	private Ruleset ruleset;
	private InputLayer kb;
	private SchemaManager manager;

	private ReasoningContext() {
	}

	public void setRuleset(Ruleset ruleset) {
		this.ruleset = ruleset;
	}

	public void setKB(InputLayer kb) {
		this.kb = kb;
	}

	public InputLayer getKB() {
		return kb;
	}

	public void init() {
		manager = new SchemaManager(kb);
	}

	public Ruleset getRuleset() {
		return ruleset;
	}

	public SchemaManager getSchemaManager() {
		return manager;
	}

	public static ReasoningContext getInstance() {
		return context;
	}
}
