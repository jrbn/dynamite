package nl.vu.cs.querypie;

import java.util.List;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.controller.CompleteRulesController;
import nl.vu.cs.querypie.reasoner.actions.controller.IncrRulesController;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.RuleParser;
import nl.vu.cs.querypie.reasoner.rules.Ruleset;
import nl.vu.cs.querypie.reasoner.support.Debugging;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reasoner {
	static final Logger log = LoggerFactory.getLogger(Reasoner.class);

	private static String deltaDir;
	private static List<Rule> rules;
	private static boolean add;
	private static boolean debug = false;
	private static String debugFile = null;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Usage: Reasoner <KB_dir> <ruleset> [--remove file with triples to remove] [--countingAlgorithm] [--debug] [--debugToFile file for debugging]");
			return;
		}
		parseArgs(args);

		try {
			Ajira arch = new Ajira();
			initAjira(args[0], arch);
			arch.startup();
			readRules(args[1]);
			initGlobalContext(arch);
			printDebug(arch);
			launchReasoning(arch);
			printDebug(arch);
			closeGlobalContext(arch);
			arch.shutdown();
		} catch (Exception e) {
			log.error("Error in the execution", e);
		}
	}

	private static void initGlobalContext(Ajira arch) {
		Ruleset set = new Ruleset(rules);
		ReasoningContext.getInstance().setRuleset(set);
		ReasoningContext.getInstance().setKB(
				(BerkeleydbLayer) arch.getContext().getInputLayer(
						Consts.DEFAULT_INPUT_LAYER_ID));
		ReasoningContext.getInstance().init();
	}

	private static void closeGlobalContext(Ajira arch) {
		ReasoningContext.getInstance().getKB().closeAll();
	}

	private static void launchReasoning(Ajira arch)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();
		if (deltaDir == null) {
			ActionsHelper.readFakeTuple(actions);
			CompleteRulesController.addToChain(actions);
		} else {
			ActionsHelper.readFakeTuple(actions);
			IncrRulesController.addToChain(actions, deltaDir, add);
		}
		job.setActions(actions);

		if (arch.amItheServer()) {
			try {
				Submission s = arch.waitForCompletion(job);
				s.printStatistics();
			} catch (Exception e) {
				log.error("The job is failed!", e);
			}
		}
	}

	private static void initAjira(String kbDir, Ajira arch) {
		Configuration conf = arch.getConfiguration();
		conf.set(Consts.STORAGE_IMPL, BerkeleydbLayer.class.getName());
		conf.set(BerkeleydbLayer.DB_INPUT, kbDir);
		conf.setInt(Consts.N_PROC_THREADS, 4);
	}

	private static void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("--remove")) {
				deltaDir = args[++i];
				add = false;
			} else if (args[i].equals("--add")) {
				deltaDir = args[++i];
				add = true;
			} else if (args[i].equals("--countingAlgorithm")) {
				ParamHandler.get().setUsingCount(true);
			} else if (args[i].equals("--debug")) {
				debug = true;
			} else if (args[i].equals("--debugFile")) {
				debugFile = args[++i];
			}
		}
	}

	private static void readRules(String fileName) {
		try {
			rules = new RuleParser().parseRules(fileName);
		} catch (Exception e) {
			log.error("Error parsing... ", e);
			log.error("Failed parsing the ruleset file. Exiting... ");
			System.exit(1);
		}
	}

	private static void printDebug(Ajira arch)
			throws ActionNotConfiguredException {
		if (debug) {
			printDerivations(arch);
		}
		if (debugFile != null) {
			printDerivationsOnFile(arch);
		}
	}

	private static void printDerivations(Ajira arch)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readFakeTuple(actions);
		Debugging.addToChain(actions);
		job.setActions(actions);

		if (arch.amItheServer()) {
			try {
				arch.waitForCompletion(job);
			} catch (Exception e) {
				log.error("The job is failed!", e);
			}
		}
	}

	private static void printDerivationsOnFile(Ajira arch)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readFakeTuple(actions);
		Debugging.addToChain(actions, debugFile);
		job.setActions(actions);

		if (arch.amItheServer()) {
			try {
				arch.waitForCompletion(job);
			} catch (Exception e) {
				log.error("The job is failed!", e);
			}
		}
	}
}
