package nl.vu.cs.dynamite;
//package nl.vu.cs.dynamite;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//
//import nl.vu.cs.dynamite.compression.TriplePartitioner;
//import nl.vu.cs.dynamite.decompression.ConvertNumberInText;
//import nl.vu.cs.dynamite.decompression.DeconstructSampleTriples;
//import nl.vu.cs.dynamite.decompression.DeconstructTriples;
//import nl.vu.cs.dynamite.decompression.ExtractTextFromPopularURIs;
//import nl.vu.cs.dynamite.decompression.ProcessCommonTextURIs;
//import nl.vu.cs.dynamite.decompression.ProcessCommonURIs;
//import nl.vu.cs.dynamite.decompression.ReconstructTriples;
//import nl.vu.cs.dynamite.decompression.URLsPartitioner;
//import nl.vu.cs.dynamite.storage.Dictionary;
//import nl.vu.cs.dynamite.storage.TripleReader;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import nl.vu.cs.ajira.Arch;
//import nl.vu.cs.ajira.actions.ActionConf;
//import nl.vu.cs.ajira.actions.ActionFactory;
//import nl.vu.cs.ajira.actions.CollectTuples;
//import nl.vu.cs.ajira.actions.CreateBranch;
//import nl.vu.cs.ajira.actions.ReadFromBucket;
//import nl.vu.cs.ajira.actions.ReadFromFile;
//import nl.vu.cs.ajira.actions.Sample;
//import nl.vu.cs.ajira.actions.WriteToFile;
//import nl.vu.cs.ajira.actions.support.FilterHiddenFiles;
//import nl.vu.cs.ajira.actions.support.HashPartitioner;
//import nl.vu.cs.ajira.chains.Chain;
//import nl.vu.cs.ajira.data.types.TInt;
//import nl.vu.cs.ajira.data.types.TString;
//import nl.vu.cs.ajira.data.types.Tuple;
//import nl.vu.cs.ajira.datalayer.files.FileLayer;
//import nl.vu.cs.ajira.storage.TupleComparator;
//import nl.vu.cs.ajira.submissions.Job;
//import nl.vu.cs.ajira.utils.Configuration;
//import nl.vu.cs.ajira.utils.Consts;
//
//public class ExportTriples {
//
//	private static Logger log = LoggerFactory.getLogger(ExportTriples.class);
//
//	private int samplingPercentage = 1;
//	private int samplingThreshold = 1000;
//	boolean ibis = false;
//
//	public static void main(String[] args) {
//		if (args.length < 3) {
//			System.out
//					.println("USAGE: ExportTriples [input data] [input dictionary] [output] [ --samplingPercentage (default 1%) --samplingThreshold (default: 1000)]");
//			return;
//		}
//
//		new ExportTriples().run(args);
//	}
//
//	private void parseArgs(String[] args) {
//		for (int i = 0; i < args.length; ++i) {
//
//			if (args[i].equals("--ibis-server")) {
//				ibis = true;
//			}
//
//			if (args[i].equals("--samplingThreshold")) {
//				samplingThreshold = Integer.valueOf(args[++i]);
//			}
//
//			if (args[i].equals("--samplingPercentage")) {
//				samplingPercentage = Integer.valueOf(args[++i]);
//			}
//		}
//	}
//
//	private void sampleTriple(List<ActionConf> actions, String input, Arch arch)
//			throws Exception {
//		// Split the input in more chunks, so that the reading
//		// is done in parallel
//		ActionConf c = ActionFactory.getActionConf(ReadFromFile.class);
//		c.setParam(ReadFromFile.CUSTOM_READER, TripleReader.class.getName());
//		actions.add(c);
//
//		// Hashcode on the entire tuple
//		actions.add(ActionFactory.getActionConf(HashPartitioner.class));
//
//		// Distribute all the lines
//		c = ActionFactory.getActionConf(SendTo.class);
//		c.setParam(SendTo.NODE_ID, SendTo.MULTIPLE);
//		c.setParam(SendTo.BUCKET_ID, 0);
//		c.setParam(SendTo.FORWARD_TUPLES, true);
//		c.setParam(SendTo.SEND_CHAIN, false);
//		c.setParam(SendTo.SORTING_FUNCTION, TupleComparator.class.getName());
//		actions.add(c);
//
//		// Sample x% percent of the triples
//		c = ActionFactory.getActionConf(Sample.class);
//		c.setParam(Sample.SAMPLE_RATE, samplingPercentage);
//		actions.add(c);
//
//		// Deconstruct sample triples
//		actions.add(ActionFactory.getActionConf(DeconstructSampleTriples.class));
//
//		// Collect them in one node
//		c = ActionFactory.getActionConf(CollectTuples.class);
//		c.setParam(CollectTuples.SORTING_FUNCTION,
//				TupleComparator.class.getName());
//		actions.add(c);
//
//		// Process the common URIs
//		c = ActionFactory.getActionConf(ProcessCommonURIs.class);
//		c.setParam(ProcessCommonURIs.SAMPLING_THRESHOLD, samplingThreshold);
//		actions.add(c);
//	}
//
//	private void sampleDictionary(List<ActionConf> actions, String inputDict,
//			Arch arch) throws Exception {
//
//		// Branch chain to process the dictionary entries
//		CreateBranch.Branch branch = new CreateBranch.Branch();
//		branch.setInputTuple(new Tuple(new TInt(FileLayer.OP_LS), new TString(
//				inputDict), new TString(FilterHiddenFiles.class.getName())));
//
//		/****** ACTIONS OF THE BRANCH ********/
//		List<ActionConf> branchActions = new ArrayList<>();
//
//		// Split the reading of the dictionary files
//		// Split the input in more chunks, so that the reading
//		// is done in parallel
//		ActionConf c = ActionFactory.getActionConf(ReadFromFile.class);
//		c.setParam(ReadFromFile.CUSTOM_READER, Dictionary.Reader.class);
//		branchActions.add(c);
//
//		// Hashcode on the entire tuple
//		branchActions.add(ActionFactory.getActionConf(HashPartitioner.class));
//
//		// Send it to multiple nodes
//		c = ActionFactory.getActionConf(SendTo.class);
//		c.setParam(SendTo.NODE_ID, SendTo.MULTIPLE);
//		c.setParam(SendTo.BUCKET_ID, 2);
//		c.setParam(SendTo.FORWARD_TUPLES, true);
//		c.setParam(SendTo.SEND_CHAIN, false);
//		c.setParam(SendTo.SORTING_FUNCTION, TupleComparator.class.getName());
//		branchActions.add(c);
//
//		// Retrieve the text of the popular URIs
//		branchActions.add(ActionFactory
//				.getActionConf(ExtractTextFromPopularURIs.class));
//
//		// Collect them in one node
//		branchActions.add(ActionFactory.getActionConf(CollectTuples.class));
//
//		// Process common text URIs
//		branchActions.add(ActionFactory
//				.getActionConf(ProcessCommonTextURIs.class));
//
//		// Add branch to the main chain
//		branch.setActions(branchActions);
//		c = ActionFactory.getActionConf(CreateBranch.class);
//		c.setParam(CreateBranch.BRANCH, branch);
//		actions.add(c);
//	}
//
//	private void run(String[] args) {
//
//		// Parse eventual optional arguments
//		parseArgs(args);
//
//		try {
//			long time = System.currentTimeMillis();
//
//			// Configure the architecture
//			Configuration conf = new Configuration();
//			conf.set(Consts.STORAGE_IMPL, FileLayer.class.getName());
//			conf.setBoolean(Consts.START_IBIS, ibis);
//
//			Arch arch = new Arch();
//			arch.startup(conf);
//
//			if (arch.isFirst()) {
//				String input = args[0];
//				String inputDict = args[1];
//				String output = args[2];
//
//				// Verify the output dir exists
//				File f = new File(output);
//				if (!f.exists()) {
//					f.mkdir();
//				}
//
//				// Init the program and launch the execution.
//				Job job = new Job();
//
//				// Set up the program
//				Chain chain = job.getMainChain();
//
//				chain.setInputTuple(new Tuple(new TInt(FileLayer.OP_LS),
//						new TString(input), new TString(FilterHiddenFiles.class
//								.getName())));
//
//				List<ActionConf> actions = new ArrayList<ActionConf>();
//				// Sample the triples
//				sampleTriple(actions, input, arch);
//
//				// Sample the dictionary
//				sampleDictionary(actions, inputDict, arch);
//
//				// Branch to read the triples in bucket 0
//				ActionConf c = ActionFactory
//						.getActionConf(ReadFromBucket.class);
//				c.setParam(ReadFromBucket.BUCKET_ID, 0);
//				c.setParam(ReadFromBucket.NODE_ID, -1);
//				actions.add(c);
//
//				// Branch to read the dictionary entries from bucket 1
//				c = ActionFactory.getActionConf(ReadFromBucket.class);
//				c.setParam(ReadFromBucket.BUCKET_ID, 2);
//				c.setParam(ReadFromBucket.NODE_ID, -1);
//				c.setParam(ReadFromBucket.BRANCH, true);
//				actions.add(c);
//
//				// Deconstruct the triples and the dictionary entries
//				actions.add(ActionFactory
//						.getActionConf(DeconstructTriples.class));
//
//				// Partition them
//				actions.add(ActionFactory.getActionConf(URLsPartitioner.class));
//
//				// Send both triple and dictionary data in one bucket to be
//				// joined together
//				c = ActionFactory.getActionConf(SendTo.class);
//				c.setParam(SendTo.NODE_ID, SendTo.MULTIPLE);
//				c.setParam(SendTo.BUCKET_ID, 4);
//				c.setParam(SendTo.SORTING_FUNCTION,
//						TupleComparator.class.getName());
//				actions.add(c);
//
//				// Convert the numbers in text
//				actions.add(ActionFactory
//						.getActionConf(ConvertNumberInText.class));
//
//				// Partition the tuples by the triple ID
//				actions.add(ActionFactory
//						.getActionConf(TriplePartitioner.class));
//
//				// Distribute the data to multiple nodes
//				c = ActionFactory.getActionConf(SendTo.class);
//				c.setParam(SendTo.NODE_ID, SendTo.MULTIPLE);
//				c.setParam(SendTo.BUCKET_ID, 5);
//				c.setParam(SendTo.SORTING_FUNCTION,
//						TupleComparator.class.getName());
//				actions.add(c);
//
//				// Reconstruct triples
//				actions.add(ActionFactory
//						.getActionConf(ReconstructTriples.class));
//
//				// Write the output on files
//				c = ActionFactory.getActionConf(WriteToFile.class);
//				c.setParam(WriteToFile.OUTPUT_DIR, output);
//				actions.add(c);
//
//				chain.addActions(actions);
//				arch.waitForCompletion(job);
//				arch.shutdown();
//				log.info("Time import: " + (System.currentTimeMillis() - time));
//				System.exit(0);
//			}
//		} catch (Exception e) {
//			log.error("Failed execution", e);
//			System.exit(1);
//		}
//	}
// }
