package nl.vu.cs.dynamite;

import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.dynamite.io.DBHandler;
import nl.vu.cs.dynamite.reasoner.rules.Ruleset;
import nl.vu.cs.dynamite.schema.SchemaManager;

public class ReasoningContext {

	private static final ReasoningContext context = new ReasoningContext();

	private Ruleset ruleset;
	private InputLayer kb;
	private SchemaManager manager;
	private DBHandler dbHandler;

	private ReasoningContext() {
	}

	public void setRuleset(Ruleset ruleset) {
		this.ruleset = ruleset;
	}

	public void setKB(InputLayer kb) {
		this.kb = kb;
	}

	public void setDBHandler(DBHandler dbHandler) {
		this.dbHandler = dbHandler;
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

	public DBHandler getDBHandler() {
		return dbHandler;
	}

	public static ReasoningContext getInstance() {
		return context;
	}

}
