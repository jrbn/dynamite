package nl.vu.cs.dynamite.compression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.chains.Chain;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.Dictionary.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessCommonURIs extends Action {

	protected static Logger log = LoggerFactory
			.getLogger(ProcessCommonURIs.class);

	public static final int S_DIR_OUTPUT = 0;
	public static final int I_SAMPLING_THRESHOLD = 1;

	private static final int MAX_THRESHOLD = 200;

	int samplingThreshold;
	String outputDictDir;
	int maxSize;

	long counter, counterAssigned;
	String currentURI;
	TString uri;
	Map<String, Long> popularURIs;
	Chain newChain = new Chain();

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DIR_OUTPUT, "S_DIR_OUTPUT", null, true);
		conf.registerParameter(I_SAMPLING_THRESHOLD, "I_SAMPLING_THRESHOLD",
				null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		outputDictDir = getParamString(S_DIR_OUTPUT);
		samplingThreshold = getParamInt(I_SAMPLING_THRESHOLD);

		currentURI = null;
		counter = 0;
		counterAssigned = 100; // Starting point
		maxSize = ConvertTextInNumber.RESERVED_SPACE - (int) counterAssigned;

		// If directory exists then we read the last counter
		File f = new File(outputDictDir);
		if (f.exists()) {
			File metaFile = new File(f, "commons-meta.txt");
			if (metaFile.exists()) {
				BufferedReader fr = new BufferedReader(new FileReader(metaFile));
				String line = fr.readLine();
				if (line != null) {
					counterAssigned = Long.valueOf(line);
				}
				fr.close();
			}
		}
		popularURIs = new HashMap<String, Long>();
	}

	private void potentiallyAddPopularURI() {
		if (currentURI != null && counter > samplingThreshold
				&& popularURIs.size() < maxSize) {
			popularURIs.put(currentURI, counterAssigned++);
		}
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		uri = (TString) inputTuple.get(0);

		if (currentURI == null || !uri.getValue().equals(currentURI)) {
			potentiallyAddPopularURI();

			// Reset counter
			counter = 0;
			currentURI = uri.getValue();
		}

		// Count the popular URIs
		counter++;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {

		potentiallyAddPopularURI();

		if (counterAssigned >= MAX_THRESHOLD) {
			throw new Exception(
					"Finished number space. Compression procedure must fail.");
		}

		// Broadcast map of popular URIs

		// Add all the previous common values
		Map<String, Long> oldValues = new HashMap<String, Long>();
		Iterator<Pair> pairs = Dictionary.readAllPairs(outputDictDir, "c");
		if (pairs != null) {
			while (pairs.hasNext()) {
				Pair pair = pairs.next();
				oldValues.put(pair.value, pair.key);
			}
		}

		if (popularURIs.size() > 0) {
			Dictionary dict = new Dictionary();
			dict.openDictionary(outputDictDir, "c");
			for (Map.Entry<String, Long> entry : popularURIs.entrySet()) {
				dict.writeNewTerm(entry.getValue(), entry.getKey());
			}
			dict.closeDictionary();

			// Write in commons-meta the last value of the counter
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
					outputDictDir, "commons-meta.txt")));
			bw.write(Long.toString(counterAssigned));
			bw.close();
		}

		popularURIs.putAll(oldValues);
		if (popularURIs.size() > 0) {
			context.putObjectInCache("popularURIs", popularURIs);
			context.broadcastCacheObjects("popularURIs");
		}

		context.incrCounter("input dictionary entries", popularURIs.size());
		context.incrCounter("popular URIs", popularURIs.size());
	}
}
