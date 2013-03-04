package nl.vu.cs.querypie;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.RuleParser;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reasoner {

	static final Logger log = LoggerFactory.getLogger(Reasoner.class);

	public static void main(String[] args) {

		if (args.length < 3) {
			System.out.println("Usage: Reasoner <KB_dir> <ruleset>");
			return;
		}

		// Start the architecture
		Ajira arch = new Ajira();
		Configuration conf = arch.getConfiguration();
		conf.set(Consts.STORAGE_IMPL, BerkeleydbLayer.class.getName());
		conf.set(BerkeleydbLayer.DB_INPUT, args[0]);
		arch.startup();

		// Parse the rules from the file
		Rule[] rules = null;
		try {
			String ruleFile = args[1];
			rules = new RuleParser().parseRules(ruleFile);
		} catch (Exception e) {
			log.error("Failed parsing the ruleset file. Exiting... ");
			return;
		}

		// Init the global context
		long t = System.currentTimeMillis();
		ReasoningContext.getInstance().init(arch, rules);
		log.info("Time to initialize the rules: "
				+ (System.currentTimeMillis() - t));

		// TODO: Launch the reasoning
	}
}
