package nl.vu.cs.querypie;

import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.schema.SchemaManager;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

public class ReasoningContext {

	private static final ReasoningContext context = new ReasoningContext();

	private Rule[] ruleset;
	private BerkeleydbLayer kb;
	private SchemaManager manager;

	private ReasoningContext() {
	}

	public void setRuleset(Rule[] ruleset) {
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
		for (Rule r : ruleset) {
			r.init(this);
		}
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

	public Rule getRule(int id) {
		return ruleset[id];
	}
}
