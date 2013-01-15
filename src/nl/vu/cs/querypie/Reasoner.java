package nl.vu.cs.querypie;

import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.RuleParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Arch;
import arch.utils.Configuration;

public class Reasoner {

	static final Logger log = LoggerFactory.getLogger(Reasoner.class);

	public static void main(String[] args) {

		if (args.length < 3) {
			System.out.println("Usage: Reasoner <KB_dir> <ruleset>");
			return;
		}

		// Create the global context
		ReasoningContext rc = new ReasoningContext();

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
		rc.init(rules);

		// Start the architecture
		Configuration conf = new Configuration();
		Arch arch = new Arch();
		arch.startup(conf);

		if (arch.isFirst()) {
			String inputDir = args[0];
			// TODO Launch the reasoning code
		}

	}
}
