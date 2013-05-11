package nl.vu.cs.dynamite.dictionary;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class DictionaryLookup extends Action {

	public static void addToChain(String inputFile, String outputFile,
			ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(DictionaryLookup.class);
		c.setParamString(S_INPUT_FILE, inputFile);
		c.setParamString(S_OUTPUT_FILE, outputFile);
		actions.add(c);
	}

	public static final int S_INPUT_FILE = 0;
	public static final int S_OUTPUT_FILE = 1;

	private final Map<String, Set<StringPos>> stringsToConsider = new HashMap<String, Set<StringPos>>();
	private FileOutputStream outputStream;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_INPUT_FILE, "input file", null, true);
		conf.registerParameter(S_OUTPUT_FILE, "output file", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		String inputFileName = getParamString(S_INPUT_FILE);
		String outputFileName = getParamString(S_OUTPUT_FILE);
		readStringToConsiderFromFile(inputFileName);
		outputStream = new FileOutputStream(new File(outputFileName));
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		String string = tuple.get(1).toString();
		if (stringsToConsider.containsKey(string)) {
			String encodedString = tuple.get(0).toString();
			for (String outputString : generateQueries(string, encodedString)) {
				outputStream.write(outputString.getBytes());
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		outputStream.close();
	}

	private void readStringToConsiderFromFile(String fileName) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] words = line.split("\t");
			String string = words[0];
			String pos = words[1];
			Set<StringPos> positions = stringsToConsider.get(string);
			if (positions == null) {
				positions = new HashSet<StringPos>();
				stringsToConsider.put(string, positions);
			}
			if (pos.equals("S")) {
				positions.add(StringPos.S);
			} else if (pos.equals("P")) {
				positions.add(StringPos.P);
			} else if (pos.equals("O")) {
				positions.add(StringPos.O);
			}
		}
		reader.close();
	}

	private List<String> generateQueries(String string, String encodedString) {
		List<String> result = new ArrayList<String>();
		for (StringPos pos : stringsToConsider.get(string)) {
			switch (pos) {
			case S:
				result.add(encodedString + " -1 -1\n");
				break;
			case P:
				result.add("-1 " + encodedString + " -1\n");
				break;
			case O:
				result.add("-1 -1 " + encodedString + "\n");
				break;
			}
		}
		return result;
	}

	private enum StringPos {
		S, P, O;
	}
}
