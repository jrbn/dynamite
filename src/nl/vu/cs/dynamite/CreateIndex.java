package nl.vu.cs.dynamite;

import java.io.File;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.actions.support.FilterHiddenFiles;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.mgmt.WebServer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.dynamite.index.BlockFlow;
import nl.vu.cs.dynamite.index.IndexPartitioner;
import nl.vu.cs.dynamite.index.PermuteTriples;
import nl.vu.cs.dynamite.index.Swap;
import nl.vu.cs.dynamite.index.WriteDictionaryToBtree;
import nl.vu.cs.dynamite.index.WriteToBtree;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.TripleFileStorage;
import nl.vu.cs.dynamite.storage.Dictionary.FilterOnlyDictionaryFiles;
import nl.vu.cs.dynamite.storage.berkeleydb.BerkeleydbLayer;
import nl.vu.cs.dynamite.storage.mapdb.MapdbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndex {

	private static Logger log = LoggerFactory.getLogger(CreateIndex.class);

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("USAGE: CreateIndex [input] [output] [--countDerivations --output <files/btree (default files)> --storage <Type of Btree> --copyOnlySchema --compressKeys --saveDictionary <dictDir>]");
			return;
		}

		new CreateIndex().run(args);
	}

	private boolean compressKeys = false;
	private boolean ibis = false;
	private String outputBtree = "files";
	private boolean countDerivations = false;
	private int nProcThreads = 4;
	private String dictionaryOutput = null;
	private static String filter = null;
	private boolean onlySchema = false;
	private Class<? extends BTreeInterface> storageBtree = BerkeleydbLayer.class;

	private Ajira ajira;

	public static class FileFilter extends FilterHiddenFiles {

		@Override
		public boolean accept(File dir, String name) {
			return name.startsWith(filter);
		}
	}

	// This class is used only for testing purposes
	public static class FilterSchema extends Action {
		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			long predicate = ((TLong) tuple.get(1)).getValue();
			if (predicate == 4 || predicate == 5 || predicate == 2
					|| predicate == 3) {
				actionOutput.output(tuple);
			}
		}
	}

	private void createIndex(ActionSequence actions) throws Exception {

		// Repartition all the triples
		ActionConf c = ActionFactory.getActionConf(PermuteTriples.class);
		c.setParamBoolean(PermuteTriples.B_COUNT, countDerivations);
		actions.add(c);

		// Repartition the triples according to the information collected during
		// the sampling
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamString(PartitionToNodes.S_PARTITIONER,
				IndexPartitioner.class.getName());

		if (countDerivations) {
			c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
					TByte.class.getName(), TLong.class.getName(),
					TLong.class.getName(), TLong.class.getName(),
					TInt.class.getName(), TInt.class.getName());
		} else {
			c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
					TByte.class.getName(), TLong.class.getName(),
					TLong.class.getName(), TLong.class.getName(),
					TInt.class.getName());
		}
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, 6);
		c.setParamBoolean(PartitionToNodes.B_SORT, true);
		actions.add(c);
	}

	private void outputTriples(ActionSequence actions, String output,
			String dictionaryOutput) throws ActionNotConfiguredException {

		if (outputBtree.equals("btree")) {
			ActionConf c = ActionFactory.getActionConf(WriteToBtree.class);
			c.setParamString(WriteToBtree.S_STORAGECLASS,
					storageBtree.getName());
			c.setParamBoolean(WriteToBtree.B_COUNT, countDerivations);
			actions.add(c);
		} else {
			ActionConf c = ActionFactory.getActionConf(WriteToFiles.class);
			c.setParamString(WriteToFiles.S_PATH, output);
			actions.add(c);
		}
	}

	private void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("--ibis-server")) {
				ibis = true;
			}

			if (args[i].equals("--countDerivations")) {
				countDerivations = true;
			}

			if (args[i].equals("--output")) {
				if (i < args.length - 1) {
					String o = args[++i];
					if (o != null && o.equals("btree")) {
						outputBtree = "btree";
					}
				} else {
					log.warn("Parameter output used incorrectly. Ignored...");
				}
			}

			if (args[i].equals("--storage")) {
				try {
					storageBtree = Class.forName(args[++i]).asSubclass(
							BTreeInterface.class);
				} catch (Exception e) {
					log.warn("Parameter --storage used incorrectly. Ignored...");
				}
			}

			if (args[i].equals("--procs")) {
				nProcThreads = Integer.parseInt(args[++i]);
			}

			if (args[i].equals("--copyOnlySchema")) {
				onlySchema = true;
			}

			if (args[i].equals("--saveDictionaryIn")) {
				dictionaryOutput = args[++i];
			}

			if (args[i].equals("--compressKeys")) {
				compressKeys = true;
			}

			if (args[i].equals("--filter")) {
				if (i < args.length - 1) {
					filter = args[++i];
				} else {
					log.warn("Parameter filter used incorrectly. Ignored...");
				}
			}
		}
	}

	private void groupAndSumCountsTriples(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(GroupBy.class);
		c.setParamInt(GroupBy.I_NPARTITIONS_PER_NODE, 6);
		c.setParamByteArray(GroupBy.BA_FIELDS_TO_GROUP, (byte) 0, (byte) 1,
				(byte) 2);
		c.setParamStringArray(GroupBy.SA_TUPLE_FIELDS, TLong.class.getName(),
				TLong.class.getName(), TLong.class.getName(),
				TInt.class.getName(), TInt.class.getName());
		actions.add(c);

		actions.add(ActionFactory
				.getActionConf(nl.vu.cs.dynamite.index.SumCounts.class));
	}

	private void run(String[] args) {
		// Parse eventual optional arguments
		parseArgs(args);
		String input = args[0];
		String output = args[1];

		try {
			ajira = new Ajira();

			// Configure the architecture
			Configuration conf = ajira.getConfiguration();
			conf.setBoolean(Consts.START_IBIS, ibis);
			conf.setInt(Consts.N_PROC_THREADS, nProcThreads);
			conf.setInt(Consts.N_MERGE_THREADS, 2);
			conf.setInt(WebServer.WEBSERVER_PORT, 50080);
			conf.setBoolean(BerkeleydbLayer.COMPRESS_KEYS, compressKeys);
			BerkeleydbLayer.setInputDir(conf, output);
			MapdbLayer.setInputDir(conf, output);

			ajira.startup();

			if (ajira.amItheServer()) {

				// Verify the output dir exists
				File f = new File(output);
				if (!f.exists()) {
					f.mkdirs();
				}

				// Init the program and launch the execution.
				Job job = new Job();

				// Set up the program
				ActionSequence actions = new ActionSequence();

				ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
				c.setParamString(ReadFromFiles.S_PATH, input);
				if (filter != null) {
					if (log.isDebugEnabled()) {
						log.debug("Setting filter class to "
								+ FileFilter.class.getName());
					}
					c.setParamString(ReadFromFiles.S_FILE_FILTER,
							FileFilter.class.getName());
					if (filter.startsWith("_count")) {
						c.setParamString(ReadFromFiles.S_CUSTOM_READER,
								TripleFileStorage.ReaderCount.class.getName());
					} else {
						c.setParamString(ReadFromFiles.S_CUSTOM_READER,
								TripleFileStorage.Reader.class.getName());
					}
				} else {
					c.setParamString(ReadFromFiles.S_CUSTOM_READER,
							TripleFileStorage.Reader.class.getName());
				}
				actions.add(c);

				if (onlySchema) {
					actions.add(ActionFactory.getActionConf(FilterSchema.class));
				}

				if (!onlySchema && dictionaryOutput != null) {
					ActionSequence sequence = new ActionSequence();
					c = ActionFactory.getActionConf(ReadFromFiles.class);
					c.setParamString(ReadFromFiles.S_PATH, dictionaryOutput);
					c.setParamString(ReadFromFiles.S_CUSTOM_READER,
							Dictionary.Reader.class.getName());
					c.setParamString(ReadFromFiles.S_FILE_FILTER,
							FilterOnlyDictionaryFiles.class.getName());
					// Set dictionary reader
					sequence.add(c);

					// Split branch
					ActionSequence sequence2 = new ActionSequence();
					sequence2.add(ActionFactory.getActionConf(Swap.class));
					c = ActionFactory
							.getActionConf(WriteDictionaryToBtree.class);
					c.setParamString(WriteDictionaryToBtree.S_STORAGECLASS,
							storageBtree.getName());
					c.setParamBoolean(WriteDictionaryToBtree.B_TEXT_NUMBER,
							true);
					sequence2.add(c);
					c = ActionFactory.getActionConf(Split.class);
					c.setParamWritable(Split.W_SPLIT, sequence2);
					c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 1);
					sequence.add(c);

					// Other write
					c = ActionFactory
							.getActionConf(WriteDictionaryToBtree.class);
					c.setParamBoolean(WriteDictionaryToBtree.B_TEXT_NUMBER,
							false);
					c.setParamString(WriteDictionaryToBtree.S_STORAGECLASS,
							storageBtree.getName());
					sequence.add(c);
					sequence.add(ActionFactory.getActionConf(BlockFlow.class));

					c = ActionFactory.getActionConf(Branch.class);
					c.setParamWritable(Branch.W_BRANCH, sequence);
					actions.add(c);
				}

				// If we have counting, I need to group the triples so that I
				// can make a sum of all the countings
				if (countDerivations && filter != null
						&& filter.startsWith("_count")) {
					groupAndSumCountsTriples(actions);
				}

				createIndex(actions);

				outputTriples(actions, output, dictionaryOutput);

				job.setActions(actions);
				Submission s = ajira.waitForCompletion(job);
				s.printStatistics();

				ajira.shutdown();
			}
		} catch (Exception e) {
			log.error("Failed execution", e);
			System.exit(1);
		}
	}
}
