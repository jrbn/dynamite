package nl.vu.cs.dynamite;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.dynamite.dictionary.DictionaryLookup;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.Dictionary.FilterOnlyDictionaryFiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetrieveDictionaryEncoding {
	static final Logger log = LoggerFactory
			.getLogger(RetrieveDictionaryEncoding.class);

	private static String dictionaryDir;
	private static String input;
	private static String output;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Usage: RetrieveDictionaryEncoding <Dict_Dir> <Input_File> <Output_File>");
			return;
		}
		dictionaryDir = args[0];
		input = args[1];
		output = args[2];

		try {
			Ajira ajira = new Ajira();
			initAjira(ajira);
			ajira.startup();
			launchProcessing(ajira);
			ajira.shutdown();
		} catch (Exception e) {
			log.error("Error in the execution", e);
		}

	}

	private static void initAjira(Ajira ajira) {
		Configuration conf = ajira.getConfiguration();
		conf.setInt(Consts.N_PROC_THREADS, 4);
	}

	private static void launchProcessing(Ajira ajira)
			throws ActionNotConfiguredException {
		ActionSequence actions = new ActionSequence();
		ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_PATH, dictionaryDir);
		c.setParamString(ReadFromFiles.S_FILE_FILTER,
				FilterOnlyDictionaryFiles.class.getName());
		c.setParamString(ReadFromFiles.S_CUSTOM_READER,
				Dictionary.Reader.class.getName());
		actions.add(c);
		DictionaryLookup.addToChain(input, output, actions);

		Job job = new Job();
		job.setActions(actions);
		Submission s = ajira.waitForCompletion(job);
		s.printStatistics();
		ajira.shutdown();
	}

}
