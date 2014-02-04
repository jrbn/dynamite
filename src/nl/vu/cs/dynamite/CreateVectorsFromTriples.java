package nl.vu.cs.dynamite;

import java.io.File;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.Sample;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.mgmt.WebServer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.dynamite.compression.BuildVectors;
import nl.vu.cs.dynamite.compression.DeconstructSampleTriples;
import nl.vu.cs.dynamite.compression.DeconstructTriples;
import nl.vu.cs.dynamite.compression.ProcessCommonURIs;
import nl.vu.cs.dynamite.parse.TripleParser;
import nl.vu.cs.dynamite.storage.BTreeInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateVectorsFromTriples {
	private static Logger log = LoggerFactory.getLogger(CreateVectorsFromTriples.class);
	
	private int samplingPercentage = 1;
	private int samplingThreshold = 1000;
	private boolean ibis = false;
	private int nProcThreads = 4;
	private boolean sampling = true;

	private static final int bucketsPerNode = 4;
	
	private Ajira ajira;
	private String kbDir = null;
	private String storageClass = null;
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("USAGE: CreateVectorsFromTriples [input] [output] [output dictionary] --samplingPercentage (default 1%) --samplingThreshold (default: 1000) ]");
			return;
		}

		new CreateVectorsFromTriples().run(args);
	}
	
	private void createVectors(ActionSequence actions, String dictOutput,
			String output) throws Exception {

		if (sampling && kbDir == null && new File(dictOutput).list().length == 0) {
			// Sample for compression
			sampleForCompression(actions, dictOutput);
		}

		// Partition the triples across the nodes
		ActionConf c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName(), TString.class.getName(),
				TString.class.getName());
		c.setParamBoolean(PartitionToNodes.B_SORT, true);
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, bucketsPerNode);
		actions.add(c);

		// Remove the duplicates
		actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));

		
		// TODO change BuildVectors to support tuples from dictionary
		/*
		if (kbDir == null) {
			// Branch to read also the existing dictionary
			File dictDir = new File(dictOutput);
			if (dictDir.exists() && dictDir.listFiles().length > 0) {
				c = ActionFactory.getActionConf(ReadFromFiles.class);
				c.setParamString(ReadFromFiles.S_PATH, dictOutput);
				c.setParamString(ReadFromFiles.S_FILE_FILTER,
						FilterOnlyDictionaryFiles.class.getName());
				c.setParamString(ReadFromFiles.S_CUSTOM_READER,
						Dictionary.Reader.class.getName());
				ActionSequence list = new ActionSequence();
				list.add(c);

				list.add(ActionFactory
						.getActionConf(ProcessDictionaryEntries.class));

				ActionConf branch = ActionFactory.getActionConf(Branch.class);
				branch.setParamWritable(Branch.W_BRANCH, list);
				actions.add(branch);
			}
		}
		*/

		// build vectors for kmeans
		c = ActionFactory.getActionConf(BuildVectors.class);
		c.setParamInt(BuildVectors.I_NPARTITIONS_PER_NODE, bucketsPerNode);
		c.setParamString(BuildVectors.S_STORAGECLASS, storageClass);
		actions.add(c);
		
		// write the vectors to file
		c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamBoolean(WriteToFiles.B_FILTER, false);
		c.setParamString(WriteToFiles.S_PATH, output);
		actions.add(c);
	}
	
	private void sampleForCompression(ActionSequence actions, String dictOutput)
			throws Exception {

		// Add this split to the main branch
		ActionConf c = ActionFactory.getActionConf(Split.class);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 4);
		actions.add(c);

		// Sample x% percent of the triples
		c = ActionFactory.getActionConf(Sample.class);
		c.setParamInt(Sample.I_SAMPLE_RATE, samplingPercentage);
		actions.add(c);

		// Deconstruct these triples in list of URIs
		c = ActionFactory.getActionConf(DeconstructSampleTriples.class);
		actions.add(c);

		// Collect them in one node
		c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				TString.class.getName());
		c.setParamBoolean(CollectToNode.B_SORT, true);
		actions.add(c);

		// Process the common URIs
		c = ActionFactory.getActionConf(ProcessCommonURIs.class);
		c.setParamString(ProcessCommonURIs.S_DIR_OUTPUT, dictOutput);
		c.setParamInt(ProcessCommonURIs.I_SAMPLING_THRESHOLD, samplingThreshold);
		actions.add(c);
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

                        if (args[i].equals("--no-sampling")) {
                            sampling = false;
                        }

                        if (args[i].equals("--sampling")) {
                            sampling = true;
                        }

			if (args[i].equals("--procs")) {
				nProcThreads = Integer.parseInt(args[++i]);
			}

			if (args[i].equals("--storage")) {
				storageClass = args[++i];
			}

			if (args[i].equals("--btree")) {
				kbDir = args[++i];
			}
		}
	}
	
	private void run(String[] args) {
		// Parse eventual optional arguments
		parseArgs(args);
		if (kbDir != null && storageClass == null) {
			storageClass = BTreeInterface.defaultStorageBtree;
		}

		try {
			ajira = new Ajira();

			// Configure the architecture
			Configuration conf = ajira.getConfiguration();
			conf.setBoolean(Consts.START_IBIS, ibis);
			conf.setInt(Consts.N_PROC_THREADS, nProcThreads);
			conf.setInt(Consts.N_MERGE_THREADS, 2);
			conf.setInt(WebServer.WEBSERVER_PORT, 50080);

			if (kbDir != null) {
				conf.set(BTreeInterface.DB_INPUT, kbDir);
			}

			ajira.startup();

			if (ajira.amItheServer()) {

				long time = System.currentTimeMillis();

				String input = args[0];
				String output = args[1];
				String dictDir = args[2];		
				
				// Verify the output dir exists
				File f = new File(output);
				if (!f.exists()) {
					f.mkdirs();
				}

				// Verify whether the dictionary input exists
				f = new File(dictDir);
				if (!f.exists()) {
					f.mkdirs();
				} else {
					log.info("There is an existing directory. Proceed with incremental compression ...");
				}

				// Init the program and launch the execution.
				Job job = new Job();

				// Set up the program
				ActionSequence actions = new ActionSequence();

				// Split the input in more chunks, so that the reading
				// is done in parallel
				ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
				c.setParamString(ReadFromFiles.S_PATH, input);
				actions.add(c);

				// Parse the triples in three strings
				actions.add(ActionFactory.getActionConf(TripleParser.class));

				createVectors(actions, dictDir, output);

				job.setActions(actions);
				Submission s = ajira.waitForCompletion(job);
				s.printStatistics();

				ajira.shutdown();
				if (s.getState().equals(Consts.STATE_FAILED)) {
					log.error("Job failed, exception:", s.getException());
					System.exit(1);
				}
				System.out.println("Time import: "
						+ (System.currentTimeMillis() - time));
				System.exit(0);
			}
		} catch (Exception e) {
			log.error("Failed execution", e);
			System.exit(1);
		}
	}
}
