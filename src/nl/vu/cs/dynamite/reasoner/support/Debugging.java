package nl.vu.cs.dynamite.reasoner.support;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.dynamite.ReasoningContext;

public class Debugging extends Action {
	public static void addToChain(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(Debugging.class);
		c.setParamBoolean(B_PRINT_ON_FILE, false);
		actions.add(c);
	}

	public static void addToChain(ActionSequence actions, String fileName)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(Debugging.class);
		c.setParamBoolean(B_PRINT_ON_FILE, true);
		c.setParamString(S_FILE_NAME, fileName);
		actions.add(c);
	}

	public static final int B_PRINT_ON_FILE = 0;
	public static final int S_FILE_NAME = 1;

	private boolean printOnFile;
	private String fileName;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_PRINT_ON_FILE, "print_on_file", false, true);
		conf.registerParameter(S_FILE_NAME, "file_name", null, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		printOnFile = getParamBoolean(B_PRINT_ON_FILE);
		if (printOnFile) {
			fileName = getParamString(S_FILE_NAME);
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (printOnFile) {
			DebuggingUtils.printDerivationsToFile(fileName, ReasoningContext
					.getInstance().getKB(), context);
		} else {
			DebuggingUtils.printDerivations(ReasoningContext.getInstance()
					.getKB(), context);
		}
	}
}
