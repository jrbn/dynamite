package nl.vu.cs.dynamite.compression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.DBType;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.WritingSession;

public class ConvertTextInNumberAfterKmeans extends Action {

	static final Logger log = LoggerFactory
			.getLogger(ConvertTextInNumberAfterKmeans.class);

	public static final int S_DIR_OUTPUT = 0;
	public static final int I_NPARTITIONS_PER_NODE = 1;
	public static final int S_STORAGECLASS = 2;
	public static final int I_INPUT_SIZE = 3;
	public static final int RESERVED_SPACE = 200;

	long counter, countInput;
	protected String uri;
	protected TString outputUri = new TString();
	TLong compressedId = new TLong();
	private Tuple tuple = TupleFactory.newTuple(outputUri, compressedId);

	Dictionary dict = null;
	String pathFile = null;
	String previousUri;
	long unique_count, start;
	int partitions, currentPartition, inputSize;
	long tid;

	private BTreeInterface in = null;
	private WritingSession t2n = null;
	private WritingSession n2t = null;
	private final byte[] cvtBuf = new byte[8];

	private void openDictionaryWriter(ActionContext context, long counter)
			throws Exception {
		dict = new Dictionary();
		dict.openDictionary(pathFile, "n", counter);

		String s = getParamString(S_STORAGECLASS);
		if (s != null) {
			in = Class.forName(getParamString(S_STORAGECLASS))
					.asSubclass(BTreeInterface.class).newInstance();
			t2n = in.openWritingSession(context, DBType.T2N);
			n2t = in.openWritingSession(context, DBType.N2T);
		}
	}

	private void closeDictionaryWriter() throws IOException {
		if (dict != null) {
			dict.closeDictionary();
			if (in != null) {
				in.close();
			}
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DIR_OUTPUT, "S_DIR_OUTPUT", null, true);
		conf.registerParameter(I_NPARTITIONS_PER_NODE,
				"I_NPARTITIONS_PER_NODE", 1, false);
		conf.registerParameter(S_STORAGECLASS, "S_STORAGECLASS", null, false);
		conf.registerParameter(I_INPUT_SIZE, "I_INPUT_SIZE", null, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		pathFile = getParamString(S_DIR_OUTPUT);
		partitions = getParamInt(I_NPARTITIONS_PER_NODE);
		inputSize = getParamInt(I_INPUT_SIZE);
		
		currentPartition = (int) context.getCounter("uri");
		counter = start = currentPartition*inputSize + RESERVED_SPACE;

		// Store the dictionary entries in a local file.
		dict = null;
		unique_count = 0;
		countInput = 0;
	}

	protected void getDetails(Tuple tuple) throws Exception {
		uri = ((TString) tuple.get(1)).getValue();
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		countInput++;
		try {
			getDetails(inputTuple);
			outputUri.setValue(uri);

			// Replace the first entry
			counter ++;
			if (dict == null) {
				openDictionaryWriter(context, start);
			}
			if (t2n != null) {
				byte[] uriBytes = uri.getBytes();
				int len = Utils
						.encodePackedLong(cvtBuf, 0, counter);
				byte[] value = Arrays.copyOf(cvtBuf, len);
				t2n.write(uriBytes, uriBytes.length, value);
				n2t.write(value, len, uriBytes);
			}
			dict.writeNewTerm(counter, uri);
			unique_count++;
			compressedId.setValue(counter);

			output.output(tuple);
		} catch (Exception e) {
			log.error("Error tralal", e);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		closeDictionaryWriter();
		context.incrCounter("not popular URIs", unique_count);
		context.incrCounter("input dictionary entries", unique_count);
		context.incrCounter("converted records in partition "
				+ currentPartition, countInput);

		// Save the counter in a file
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				pathFile, currentPartition + "-meta.txt")));
		writer.write(Long.toString(counter));
		writer.close();
	}
}
