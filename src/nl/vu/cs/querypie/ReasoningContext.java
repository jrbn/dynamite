package nl.vu.cs.querypie;

import nl.vu.cs.querypie.reasoner.rules.Ruleset;
import nl.vu.cs.querypie.schema.SchemaManager;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

public class ReasoningContext {

	private static final ReasoningContext context = new ReasoningContext();

	private Ruleset ruleset;
	private BerkeleydbLayer kb;
	private SchemaManager manager;

	private ReasoningContext() {
	}

	public void setRuleset(Ruleset ruleset) {
		this.ruleset = ruleset;
	}

	public void setKB(BerkeleydbLayer kb) {
		this.kb = kb;
	}

	public BerkeleydbLayer getKB() {
		return kb;
	}

	public void init() {
		manager = new SchemaManager(kb);
		ruleset.init(this);
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
