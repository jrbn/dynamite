package nl.vu.cs.querypie;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.RuleParser;
import nl.vu.cs.querypie.reasoner.rules.Ruleset;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reasoner {

	static final Logger log = LoggerFactory.getLogger(Reasoner.class);

	private static String deltaDir = null;

	private static void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("--remove")) {
				deltaDir = args[++i];
			}
		}
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Usage: Reasoner <KB_dir> <ruleset> [--remove file with triples to remove]");
			return;
		}
		parseArgs(args);

		// Start the architecture
		Ajira arch = new Ajira();
		Configuration conf = arch.getConfiguration();
		conf.set(Consts.STORAGE_IMPL, BerkeleydbLayer.class.getName());
		conf.set(BerkeleydbLayer.DB_INPUT, args[0]);
		conf.setInt(Consts.N_PROC_THREADS, 4);
		arch.startup();

		// Parse the rules from the file
		Rule[] rules = null;
		try {
			String ruleFile = args[1];
			rules = new RuleParser().parseRules(ruleFile);
		} catch (Exception e) {
			log.error("Error parsing... ", e);
			log.error("Failed parsing the ruleset file. Exiting... ");
			System.exit(1);
		}

		// Init the global context
		Ruleset set = new Ruleset(rules);
		ReasoningContext.getInstance().setRuleset(set);
		ReasoningContext.getInstance().setKB(
				(BerkeleydbLayer) arch.getContext().getInputLayer(
						Consts.DEFAULT_INPUT_LAYER_ID));

		// The first time we initialize all the rules
		ReasoningContext.getInstance().init();

		// Launch the reasoning
		Job job = new Job();
		List<ActionConf> actions = new ArrayList<ActionConf>();
		ActionsHelper.readFakeTuple(actions);
		if (deltaDir == null) {
			ActionsHelper.runRulesController(1, actions);
		} else {
			ActionsHelper.runIncrRulesController(actions, deltaDir);
		}
		job.setActions(actions);

		if (arch.amItheServer()) {
			try {
				Submission s = arch.waitForCompletion(job);
				s.printStatistics();
				ReasoningContext.getInstance().getKB().closeAll();
			} catch (Exception e) {
				log.error("The job is failed!", e);
			}
		}

		arch.shutdown();
	}
}
