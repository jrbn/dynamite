package nl.vu.cs.dynamite;

import java.io.File;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.Sample;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.dynamite.compression.TriplePartitioner;
import nl.vu.cs.dynamite.decompression.ConvertNumberInText;
import nl.vu.cs.dynamite.decompression.DeconstructSampleTriples;
import nl.vu.cs.dynamite.decompression.DeconstructTriples;
import nl.vu.cs.dynamite.decompression.ExtractTextFromPopularURIs;
import nl.vu.cs.dynamite.decompression.ProcessCommonTextURIs;
import nl.vu.cs.dynamite.decompression.ProcessCommonURIs;
import nl.vu.cs.dynamite.decompression.ReconstructTriples;
import nl.vu.cs.dynamite.decompression.URLsPartitioner;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.Dictionary.FilterOnlyDictionaryFiles;
import nl.vu.cs.dynamite.storage.TripleFileStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Decompress {

	private static Logger log = LoggerFactory.getLogger(Decompress.class);

	private int samplingPercentage = 1;
	private int samplingThreshold = 1000;
	private int nProcThreads = 4;
	private final int bucketsPerNode = 4;
	boolean ibis = false;

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("USAGE: Decompress [input data] [input dictionary] [output] [ --samplingPercentage (default 1%) --samplingThreshold (default: 1000)]");
			return;
		}

		new Decompress().run(args);
	}

	private void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {

			if (args[i].equals("--ibis-server")) {
				ibis = true;
			}

			if (args[i].equals("--samplingThreshold")) {
				samplingThreshold = Integer.valueOf(args[++i]);
			}

			if (args[i].equals("--samplingPercentage")) {
				samplingPercentage = Integer.valueOf(args[++i]);
			}

			if (args[i].equals("--procs")) {
				nProcThreads = Integer.parseInt(args[++i]);
			}

		}
	}

	private void sampleTriple(ActionSequence actions, String input, Ajira arch)
			throws Exception {
		// Split the input in more chunks, so that the reading
		// is done in parallel
		ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_CUSTOM_READER,
				TripleFileStorage.Reader.class.getName());
		c.setParamString(ReadFromFiles.S_PATH, input);
		actions.add(c);
		// Reads tuples: <TLong, TLong, TLong>

		// Hashcode on the entire tuple
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamBoolean(PartitionToNodes.B_SORT, true);
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, bucketsPerNode);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TLong.class.getName());
		actions.add(c);
		//
		// c = ActionFactory.getActionConf(WriteToBucket.class);
		// c.setParamInt(WriteToBucket.I_BUCKET_ID, 0);
		// c.setParamStringArray(WriteToBucket.SA_TUPLE_FIELDS,
		// TLong.class.getName(), TLong.class.getName(),
		// TLong.class.getName());
		// list.add(c);
		// Try something else: split the rest off.

		// Add this split to the main branch
		c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 4);
		actions.add(c);

		// Sample x% percent of the triples
		c = ActionFactory.getActionConf(Sample.class);
		c.setParamInt(Sample.I_SAMPLE_RATE, samplingPercentage);
		actions.add(c);
		// Still: <TLong, TLong, TLong>

		// Deconstruct sample triples
		actions.add(ActionFactory.getActionConf(DeconstructSampleTriples.class));
		// Now: <TLong>.

		// Collect them in one node
		c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				TLong.class.getName());
		c.setParamBoolean(CollectToNode.B_SORT, true);
		actions.add(c);

		// Process the common URIs
		c = ActionFactory.getActionConf(ProcessCommonURIs.class);
		c.setParamInt(ProcessCommonURIs.I_SAMPLING_THRESHOLD, samplingThreshold);
		actions.add(c);
		// Nothing comes out of this part of the split.

		/*
		 * c = ActionFactory.getActionConf(Sample.class);
		 * c.setParamInt(Sample.I_SAMPLE_RATE, 101); actions.add(c);
		 */
		// Delivers: <TLong, TLong, TLong>
	}

	private void sampleDictionary(ActionSequence actions, String inputDict,
			Ajira arch) throws Exception {

		ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_PATH, inputDict);
		c.setParamString(ReadFromFiles.S_FILE_FILTER,
				FilterOnlyDictionaryFiles.class.getName());
		c.setParamString(ReadFromFiles.S_CUSTOM_READER,
				Dictionary.Reader.class.getName());
		ActionSequence list = new ActionSequence();
		list.add(c);
		// <TLong, TString>

		// Hashcode on the entire tuple
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, bucketsPerNode);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TLong.class.getName(), TString.class.getName());
		list.add(c);
		//
		// c = ActionFactory.getActionConf(WriteToBucket.class);
		// c.setParamInt(WriteToBucket.I_BUCKET_ID, 2);
		// c.setParamStringArray(WriteToBucket.SA_TUPLE_FIELDS,
		// TLong.class.getName(), TString.class.getName());
		// list.add(c);
		// Try something else: split the stuff below.

		// Add this split to the main branch
		c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 3);
		list.add(c);

		// Retrieve the text of the popular URIs
		list.add(ActionFactory.getActionConf(ExtractTextFromPopularURIs.class));
		// Still <TLong, TString>

		// Collect them in one node
		c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				TLong.class.getName(), TString.class.getName());
		list.add(c);

		// Process common text URIs
		list.add(ActionFactory.getActionConf(ProcessCommonTextURIs.class));
		// Nothing comes out of this split

		// A dummy action to catch the Split. If you remove this, the
		// application hangs. Bug? Maybe a split cannot end behind
		// the last action of a branch. TODO: Check!
		c = ActionFactory.getActionConf(Sample.class);
		c.setParamInt(Sample.I_SAMPLE_RATE, 101);
		list.add(c);

		c = ActionFactory.getActionConf(Branch.class);
		c.setParamWritable(Branch.W_BRANCH, list);
		actions.add(c);

		// Delivers: <TLong, TString>
	}

	private void run(String[] args) {

		// Parse eventual optional arguments
		parseArgs(args);

		try {
			long time = System.currentTimeMillis();

			// Configure the architecture
			Ajira arch = new Ajira();
			Configuration conf = arch.getConfiguration();
			conf.setBoolean(Consts.START_IBIS, ibis);
			conf.setInt(Consts.N_PROC_THREADS, nProcThreads);

			arch.startup();

			if (arch.amItheServer()) {
				String input = args[0];
				String inputDict = args[1];
				String output = args[2];

				// Verify the output dir exists
				File f = new File(output);
				if (!f.exists()) {
					f.mkdir();
				}

				// Init the program and launch the execution.
				Job job = new Job();

				// Set up the program
				ActionSequence actions = new ActionSequence();

				// Sample the triples
				sampleTriple(actions, input, arch);

				// Branch to sample the dictionary
				sampleDictionary(actions, inputDict, arch);

				// Now we have both dictionary entries and triples.

				// Deconstruct the triples and the dictionary entries
				actions.add(ActionFactory
						.getActionConf(DeconstructTriples.class));
				// <TLong, TByte, TByte, TLong, TString>

				ActionConf c = ActionFactory
						.getActionConf(PartitionToNodes.class);
				c.setParamString(PartitionToNodes.S_PARTITIONER,
						URLsPartitioner.class.getName());
				c.setParamBoolean(PartitionToNodes.B_SORT, true);
				c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE,
						bucketsPerNode);
				c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
						TLong.class.getName(), TByte.class.getName(),
						TByte.class.getName(), TLong.class.getName(),
						TString.class.getName());
				actions.add(c);

				// Convert the numbers in text
				actions.add(ActionFactory
						.getActionConf(ConvertNumberInText.class));

				// <TLong, TByte, TString>

				// Partition the tuples by the triple ID
				c = ActionFactory.getActionConf(PartitionToNodes.class);
				c.setParamString(PartitionToNodes.S_PARTITIONER,
						TriplePartitioner.class.getName());
				c.setParamBoolean(PartitionToNodes.B_SORT, true);
				c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE,
						bucketsPerNode);
				c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
						TLong.class.getName(), TByte.class.getName(),
						TString.class.getName());
				actions.add(c);

				// Reconstruct triples
				actions.add(ActionFactory
						.getActionConf(ReconstructTriples.class));

				// Write the output on files
				c = ActionFactory.getActionConf(WriteToFiles.class);
				c.setParamString(WriteToFiles.S_PATH, output);
				actions.add(c);

				job.setActions(actions);
				arch.waitForCompletion(job);
				arch.shutdown();
				log.info("Time import: " + (System.currentTimeMillis() - time));
				System.exit(0);
			}
		} catch (Exception e) {
			log.error("Failed execution", e);
			System.exit(1);
		}
	}
}
