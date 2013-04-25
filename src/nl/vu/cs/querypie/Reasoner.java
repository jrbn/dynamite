package nl.vu.cs.querypie;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.mgmt.WebServer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.controller.CompleteRulesController;
import nl.vu.cs.querypie.reasoner.actions.controller.IncrRulesController;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.RuleParser;
import nl.vu.cs.querypie.reasoner.rules.Ruleset;
import nl.vu.cs.querypie.reasoner.support.Debugging;
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;
import nl.vu.cs.querypie.storage.mapdb.MapdbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reasoner {
	static final Logger log = LoggerFactory.getLogger(Reasoner.class);

	private static String deltaDir;
	private static List<Rule> rules;
	private static boolean add;
	private static boolean debug = false;
	private static String debugFile = null;
	private static String lastStepFile = null;
	private static String storage = "btree";
	private static Class<? extends InputLayer> storageClass = BerkeleydbLayer.class;

	private static int nProcThreads = 4;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Usage: Reasoner <KB_dir> <ruleset> [--remove|--add <diff_file>] [--countingAlgorithm] [--debug] [--debugToFile <file>] [--lastStepFile <file>] [--writeCopyAt <dir>]");
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
				arch.getContext().getInputLayer(storageClass));
		ReasoningContext.getInstance().init();
	}

	private static void closeGlobalContext(Ajira arch) {
		ReasoningContext.getInstance().getKB().close();
	}

	private static void launchReasoning(Ajira arch)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();
		if (deltaDir == null) {
			ActionsHelper.readFakeTuple(actions);
			CompleteRulesController.addToChain(actions);
		} else {
			loadLastStepFromFile();
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
		writeLastStepToFile();
	}

	private static void initAjira(String kbDir, Ajira arch) {
		Configuration conf = arch.getConfiguration();
		if (storage.equals("btree")) {
			storageClass = BerkeleydbLayer.class;
			conf.set(BerkeleydbLayer.DB_INPUT, kbDir);
		} else if (storage.equals("mapdb")) {
			storageClass = MapdbLayer.class;
			conf.set(MapdbLayer.DB_INPUT, kbDir);
		}
		InputLayer.setDefaultInputLayerClass(storageClass, conf);
		conf.setInt(Consts.N_PROC_THREADS, nProcThreads);
		conf.setInt(WebServer.WEBSERVER_PORT, 50080);
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
			} else if (args[i].equals("--lastStepFile")) {
				lastStepFile = args[++i];
			} else if (args[i].equals("--storage")) {
				storage = args[++i];
			} else if (args[i].equals("--procs")) {
				nProcThreads = Integer.parseInt(args[++i]);
			} else if (args[i].equals("--writeCopyAt")) {
				ParamHandler.get().setCopyDir(args[++i]);
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

	private static void loadLastStepFromFile() {
		if (lastStepFile == null) {
			return;
		}
		try {
			FileInputStream fis = new FileInputStream(new File(lastStepFile));
			int lastStep = fis.read();
			fis.close();
			ParamHandler.get().setLastStep(lastStep);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void writeLastStepToFile() {
		if (lastStepFile == null) {
			return;
		}
		try {
			FileOutputStream fos = new FileOutputStream(new File(lastStepFile));
			int lastStep = ParamHandler.get().getLastStep();
			fos.write(lastStep);
			fos.close();
			ParamHandler.get().setLastStep(lastStep);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
