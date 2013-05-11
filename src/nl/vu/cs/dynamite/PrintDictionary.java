package nl.vu.cs.dynamite;

import java.io.File;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.Dictionary.FilterOnlyDictionaryFiles;

public class PrintDictionary extends Action {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("USAGE: PrintDictionary <dir>");
			return;
		}
		
		ActionSequence list = new ActionSequence();
		
		File dictDir = new File(args[0]);
		if (dictDir.exists() && dictDir.listFiles().length > 0) {
			ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
			c.setParamString(ReadFromFiles.S_PATH, dictDir.getAbsolutePath());
			c.setParamString(ReadFromFiles.S_FILE_FILTER,
					FilterOnlyDictionaryFiles.class.getName());
			c.setParamString(ReadFromFiles.S_CUSTOM_READER,
					Dictionary.Reader.class.getName());
			list.add(c);

			list.add(ActionFactory.getActionConf(PrintDictionary.class));
			
			Ajira ajira = new Ajira();

			ajira.startup();

			if (ajira.amItheServer()) {
				// Init the program and launch the execution.
				Job job = new Job();

				job.setActions(list);
				ajira.waitForCompletion(job);

				ajira.shutdown();
			}
		} else {
			System.err.println("Dictionary directory does not contain dictionary.");
			return;
		}
	}
	

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		System.out.println(tuple.toString());
	}
}
