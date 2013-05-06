package nl.vu.cs.querypie;

import java.io.File;
import java.io.IOException;
import java.util.List;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.mgmt.WebServer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.io.DBHandler;
import nl.vu.cs.querypie.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.RuleParser;
import nl.vu.cs.querypie.reasoner.rules.Ruleset;
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;
import nl.vu.cs.querypie.storage.mapdb.MapdbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListDB {
	static final Logger log = LoggerFactory.getLogger(ListDB.class);

	private static List<Rule> rules;
	private static String storage = "btree";
	private static boolean compressKeys = false;
	private static Class<? extends InputLayer> storageClass = BerkeleydbLayer.class;

	private static int nProcThreads = 4;

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage: ListDB <KB_dir> <ruleset> <destination-dir> [ options ]");
			return;
		}
		parseArgs(args);

		try {
			Ajira arch = new Ajira();
			initAjira(args[0], arch);
			arch.startup();
			readRules(args[1]);
			initGlobalContext(arch);
			launchLister(arch, args[2]);
			closeGlobalContext(arch);
			arch.shutdown();
		} catch (Exception e) {
			log.error("Error in the execution", e);
		}
	}

	private static void initGlobalContext(Ajira arch) {
		Ruleset set = new Ruleset(rules);
		ReasoningContext.getInstance().setRuleset(set);
		ReasoningContext.getInstance().setKB(arch.getContext().getInputLayer(storageClass));
		ReasoningContext.getInstance().setDBHandler(new DBHandler());
		ReasoningContext.getInstance().init();
	}

	private static void closeGlobalContext(Ajira arch) {
		ReasoningContext.getInstance().getKB().close();
		ReasoningContext.getInstance().getDBHandler().close();
	}

	private static void launchLister(Ajira arch, String dest) throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readEverythingFromBTree(actions);
		ActionConf c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamString(WriteToFiles.S_PATH, dest);
		actions.add(c);
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

	private static void initAjira(String kbDir, Ajira arch) throws IOException {

		// Check whether the input dir exists
		if (!new File(kbDir).exists()) {
			throw new IOException("Input dir " + kbDir + " does not exist!");
		}

		Configuration conf = arch.getConfiguration();
		if (storage.equals("btree")) {
			storageClass = BerkeleydbLayer.class;
			conf.set(BerkeleydbLayer.DB_INPUT, kbDir);
			conf.setBoolean(BerkeleydbLayer.COMPRESS_KEYS, compressKeys);
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
			if (args[i].equals("--countDerivations")) {
				ParamHandler.get().setUsingCount(true);
			} else if (args[i].equals("--storage")) {
				storage = args[++i];
			} else if (args[i].equals("--procs")) {
				nProcThreads = Integer.parseInt(args[++i]);
			} else if (args[i].equals("--compressKeys")) {
				compressKeys = true;
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
}
