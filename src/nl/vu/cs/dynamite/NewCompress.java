package nl.vu.cs.dynamite;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.ReadFromBucket;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.Sample;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.WriteToBucket;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TDoubleArray;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.examples.KMeans.ParseVectors;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.mgmt.WebServer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.dynamite.compression.BuildVectors;
import nl.vu.cs.dynamite.compression.ConvertTextInNumber;
import nl.vu.cs.dynamite.compression.DeconstructSampleTriples;
import nl.vu.cs.dynamite.compression.DeconstructTriples;
import nl.vu.cs.dynamite.compression.ProcessCommonURIs;
import nl.vu.cs.dynamite.compression.ProcessDictionaryEntries;
import nl.vu.cs.dynamite.compression.ReconstructTriples;
import nl.vu.cs.dynamite.compression.TriplePartitioner;
import nl.vu.cs.dynamite.compression.URLsPartitioner;
import nl.vu.cs.dynamite.parse.TripleParser;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.Dictionary.FilterOnlyDictionaryFiles;
import nl.vu.cs.dynamite.storage.TripleFileStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewCompress {

	private static Logger log = LoggerFactory.getLogger(Compress.class);

	public static final String CENTERS = "KMeansMapper.centers";
	public static final String NEW_CENTERS = "KMeansMapper.newcenters";
	private static final int PARTITIONS_PER_NODE = 4;
	private static final int NUMBER_OF_CENTERS = 10;
	private static final int TUPLE_SIZE = 6;
	private static final int CONVERGENCE_THRESHOLD = 40;
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("USAGE: Compress [input] [output] [output dictionary] --samplingPercentage (default 1%) --samplingThreshold (default: 1000) ]");
			return;
		}

		new NewCompress().run(args);
	}

	private int samplingPercentage = 1;
	private int samplingThreshold = 1000;
	private boolean ibis = false;
	private int nProcThreads = 4;
	private boolean sampling = true;

	private static final int bucketsPerNode = 4;

	private Ajira ajira;
	private String kbDir = null;
	private String storageClass = null;

	// support methods
	private static final double euclideanDistance(Tuple t1, Tuple t2) {
		int i = 1;
		double sum = 0, diff;
		
		long diffTripleId = ((TLong) t1.get(i)).getValue() -
				((TLong) t2.get(i++)).getValue();
		sum += diffTripleId * diffTripleId;
		
		int diffPosition = ((TByte) t1.get(i)).getValue() -
				((TByte) t2.get(i++)).getValue();
		sum += diffPosition * diffPosition;
		
		int diffHashDomain = ((TInt) t1.get(i)).getValue() -
				((TInt) t2.get(i++)).getValue();
		sum += diffHashDomain * diffHashDomain;
		
		int diffHashPath = ((TInt) t1.get(i)).getValue() -
				((TInt) t2.get(i++)).getValue();
		sum += diffHashPath * diffHashPath;
		
		if (((TByte) t1.get(i-3)).getValue() == 1 &&
				((TByte) t2.get(i-3)).getValue() == 1
		) {
			int diffHashObject = ((TInt) t1.get(i)).getValue() - 
					((TInt) t2.get(i++)).getValue();
			sum += diffHashObject * diffHashObject;
		}
		
		return Math.sqrt(sum);
	}
	
	private static final void sum(Tuple t1, Tuple t2) {
		TLong tripleId = new TLong();
		TByte position = new TByte();
		TInt domainHash = new TInt();
		TInt pathHash = new TInt();
		TInt objectHash = new TInt();
		t1.set(t1.get(0), tripleId, position, domainHash, pathHash, objectHash);
		tripleId.setValue(((TLong) t1.get(1)).getValue() +
				((TLong) t2.get(1)).getValue());
		position.setValue(((TByte) t1.get(2)).getValue() +
				((TByte) t2.get(2)).getValue());
		domainHash.setValue(((TInt) t1.get(3)).getValue() +
				((TInt) t2.get(3)).getValue());
		pathHash.setValue(((TInt) t1.get(4)).getValue() +
				((TInt) t2.get(4)).getValue());
		objectHash.setValue(((TInt) t1.get(5)).getValue() +
				((TInt) t2.get(5)).getValue());
	}

	// TODO add set and get methods to simple data such that
	// they can be used without casting to tint, tlong etc,
	private static final void divide(Tuple t, int s) {
		int i = 1;
		TLong tripleId = (TLong) t.get(i++);
		TByte position = (TByte) t.get(i++);
		TInt domainHash = (TInt) t.get(i++);
		TInt pathHash = (TInt) t.get(i++);
		TInt objectHash = (TInt) t.get(i++);
		
		tripleId.setValue(tripleId.getValue()/s);
		position.setValue(position.getValue()/s);
		domainHash.setValue(domainHash.getValue()/s);
		pathHash.setValue(pathHash.getValue()/s);
		objectHash.setValue(objectHash.getValue()/s);
		
		t.set(t.get(0), tripleId, position, domainHash, pathHash, objectHash);
	}
	
	private void compress(ActionSequence actions, String dictOutput,
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

		// kmeans starts here
		c = ActionFactory.getActionConf(BuildVectors.class);
		c.setParamInt(DeconstructTriples.I_NPARTITIONS_PER_NODE, bucketsPerNode);
		c.setParamString(BuildVectors.S_STORAGECLASS, storageClass);
		actions.add(c);
		
		c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamString(WriteToFiles.S_PATH, "temp");
		actions.add(c);
		
		c = ActionFactory.getActionConf(GenerateRandomCentroidsAndStartKmeans.class);
		c.setParamString(GenerateRandomCentroidsAndStartKmeans.S_OUTPUT_DIR,
			output);
		c.setParamString(GenerateRandomCentroidsAndStartKmeans.S_DICTIONARY,
			dictOutput);
		actions.add(c);
	}

	public static void compressCont(ActionSequence actions, String dictionary,
			String output) throws Exception {
		// Convert text in numbers
		ActionConf c = ActionFactory.getActionConf(ConvertTextInNumber.class);
		c.setParamInt(ConvertTextInNumber.I_NPARTITIONS_PER_NODE,
				bucketsPerNode);
		c.setParamString(ConvertTextInNumber.S_DIR_OUTPUT, dictionary);
		actions.add(c);

		// Distribute them again
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamString(PartitionToNodes.S_PARTITIONER,
				TriplePartitioner.class.getName());
		c.setParamBoolean(PartitionToNodes.B_SORT, true);
		c.setParamByteArray(PartitionToNodes.IA_SORTING_FIELDS, (byte) 0);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TByte.class.getName());
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, bucketsPerNode);
		actions.add(c);

		// Reconstruct triples
		actions.add(ActionFactory.getActionConf(ReconstructTriples.class));

		c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamString(WriteToFiles.S_PATH, output);
		c.setParamString(WriteToFiles.S_CUSTOM_WRITER,
				TripleFileStorage.Writer.class.getName());
		c.setParamString(WriteToFiles.S_PREFIX_FILE, "import");
		c.setParamBoolean(WriteToFiles.B_FILTER, false);
		c.setParamBoolean(WriteToFiles.B_OVERWRITE_FILES, false);
		actions.add(c);
	}
	
	private static final void kmeans(ActionSequence actions, String dictionary,
			String output) throws ActionNotConfiguredException {
		System.out.println("kmeans");
		// Find the closest center
		actions.add(ActionFactory.getActionConf(FindClosestCenter.class));

		// Groups the pairs by center
		ActionConf action = ActionFactory.getActionConf(GroupBy.class);
		action.setParamStringArray(GroupBy.SA_TUPLE_FIELDS,
				TInt.class.getName(), TString.class.getName(),
				TLong.class.getName(), TByte.class.getName(),
				TInt.class.getName(), TInt.class.getName(),
				TInt.class.getName());
		action.setParamByteArray(GroupBy.BA_FIELDS_TO_GROUP, (byte) 0);
		action.setParamInt(GroupBy.I_NPARTITIONS_PER_NODE,
				PARTITIONS_PER_NODE);
		actions.add(action);

		// Update the clusters
		actions.add(ActionFactory.getActionConf(UpdateClusters.class));

		// Write the results in a bucket
		action = ActionFactory.getActionConf(WriteToBucket.class);
		action.setParamStringArray(WriteToBucket.SA_TUPLE_FIELDS,
				TInt.class.getName(), TString.class.getName(),
				TLong.class.getName(), TByte.class.getName(),
				TInt.class.getName(), TInt.class.getName(),
				TInt.class.getName());
		actions.add(action);

		// Collect to one node
		action = ActionFactory.getActionConf(CollectToNode.class);
		action.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				TInt.class.getName(), TInt.class.getName());
		actions.add(action);

		// Update the centroids
		action = ActionFactory.getActionConf(UpdateCentroids.class);
		action.setParamString(UpdateCentroids.S_OUTPUT_DIR, output);
		action.setParamString(UpdateCentroids.S_DICTIONARY, dictionary);
		actions.add(action);
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

				compress(actions, dictDir, output);

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
	
	/* Parse a vector encodend in a string */
	public static class ParseVectors extends Action {
		private TString url = new TString();
		private TLong tripleId = new TLong();
		private TByte position = new TByte();
		private TInt domainHash = new TInt();
		private TInt pathHash = new TInt();
		private TInt objectHash = new TInt(0);
		
		private Tuple outputTuple = TupleFactory.newTuple(url, tripleId, position,
				domainHash, pathHash, objectHash);
		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			String line = ((TString) tuple.get(0)).getValue();
			String[] components = line.split(" ");
			
			url.setValue(components[0]);
			tripleId.setValue(Long.parseLong(components[1]));
			position.setValue(Integer.parseInt(components[2]));
			domainHash.setValue(Integer.parseInt(components[3]));
			pathHash.setValue(Integer.parseInt(components[4]));
			objectHash.setValue(Integer.parseInt(components[5]));
			
			actionOutput.output(outputTuple);
		}
	}
	
	public static class GenerateRandomCentroidsAndStartKmeans extends Action {
		public static final int S_OUTPUT_DIR = 0;
		public static final int S_DICTIONARY = 1;
		private Map<Integer, Tuple> centers;
		private String outputDir;
		private String dictionary;
		int counter;
		Random r;

		@Override
		protected void registerActionParameters(ActionConf conf) {
			conf.registerParameter(S_OUTPUT_DIR, "", null, true);
			conf.registerParameter(S_DICTIONARY, "", null, true);
		}

		@Override
		public void startProcess(ActionContext context) throws Exception {
			centers = new HashMap<Integer, Tuple>();
			outputDir = getParamString(S_OUTPUT_DIR);
			dictionary = getParamString(S_DICTIONARY);
			counter = 0;
			r = new Random();
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
		}
			

		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			

			if (context.isPrincipalBranch()) {
				for (int i = 0; i < NUMBER_OF_CENTERS; i++) {
					TByte pos = new TByte(r.nextInt(2)+1);
					TLong tripleId = new TLong((long)r.nextInt(Integer.MAX_VALUE));
					TInt hashDomain = new TInt(r.nextInt(Integer.MAX_VALUE));
					TInt hashPath = new TInt(r.nextInt(Integer.MAX_VALUE));
					TInt hashObject = new TInt(0);
					if(pos.getValue() == 1) {
						hashObject.setValue(r.nextInt(Integer.MAX_VALUE));
					}
					Tuple center = TupleFactory.newTuple(new TString(), tripleId, pos,
							hashDomain, hashPath, hashObject);
					centers.put(counter++, center);
					System.out.println(center);
				}
				
				context.putObjectInCache(CENTERS, centers);
				context.broadcastCacheObjects(CENTERS);
				
				ActionSequence actions = new ActionSequence();
				
				// Read the input files
				ActionConf action = ActionFactory
						.getActionConf(ReadFromFiles.class);
				action.setParamString(ReadFromFiles.S_PATH, "temp");
				actions.add(action);

				// Parse the textual lines into vectors
				actions.add(ActionFactory.getActionConf(ParseVectors.class));
				
				// Start the k-means procedure with a branch
				kmeans(actions, dictionary, outputDir);
				actionOutput.branch(actions);
			}

			outputDir = null;
			dictionary = null;
			centers = null;
		}
	}
	
	/* For each vector, find the closest center */
	public static class FindClosestCenter extends Action {

		private Set<Map.Entry<Integer, Tuple>> centers = null;
		private final TInt key = new TInt();

		@SuppressWarnings("unchecked")
		@Override
		public void startProcess(ActionContext context) throws Exception {
			centers = ((Map<Integer, Tuple>) context
					.getObjectFromCache(CENTERS)).entrySet();
		}

		@Override
		public void process(Tuple t, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			int tupleSize = t.getNElements();
			Tuple tuple = TupleFactory.newTuple(t.get(tupleSize-6), t.get(tupleSize-5),
					t.get(tupleSize-4), t.get(tupleSize-3),
					t.get(tupleSize-2), t.get(tupleSize-1));
			Tuple nearest = null;
			double nearestDistance = Double.MAX_VALUE;
			int index = -1;
			for (Map.Entry<Integer, Tuple> c : centers) {
				double dist = euclideanDistance(tuple, c.getValue());
				if (nearest == null) {
					nearest = c.getValue();
					nearestDistance = dist;
					index = c.getKey();
				} else {
					if (nearestDistance > dist) {
						nearest = c.getValue();
						nearestDistance = dist;
						index = c.getKey();
					}
				}
			}
			key.setValue(index);
			Tuple output = TupleFactory.newTuple(key, tuple.get(0),
					tuple.get(1), tuple.get(2), tuple.get(3), tuple.get(4),
					tuple.get(5));
			actionOutput.output(output);
		}

		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			centers = null;
		}

	}
	
	public static class UpdateClusters extends Action {
		private final TInt key = new TInt();
		private final Tuple value = TupleFactory.newTuple(); 
		private Map<Integer, Tuple> newCenters = null;

		@Override
		public void startProcess(ActionContext context) throws Exception {
			newCenters = new HashMap<Integer, Tuple>();
			key.setValue(-1);
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			List<Tuple> vectorList = new ArrayList<Tuple>();
			Tuple newCenter = null;
			TBag values = (TBag) tuple.get(1);
			for (Tuple value : values) {
				TString url = new TString();
				TLong tripleId = new TLong();
				TByte position = new TByte();
				TInt domainHash = new TInt();
				TInt pathHash = new TInt();
				TInt objectHash = new TInt();
				
				Tuple aTuple = TupleFactory.newTuple(url, tripleId,
						position, domainHash, pathHash, objectHash);
				url.setValue(((TString) value.get(0)).getValue());
				tripleId.setValue(((TLong) value.get(1)).getValue());
				position.setValue(((TByte) value.get(2)).getValue());
				domainHash.setValue(((TInt) value.get(3)).getValue());
				pathHash.setValue(((TInt) value.get(4)).getValue());
				objectHash.setValue(((TInt) value.get(5)).getValue());
				Tuple t = TupleFactory.newTuple();
				aTuple.copyTo(t);
				vectorList.add(t);
				if (newCenter == null) {
					newCenter = TupleFactory.newTuple();
					aTuple.copyTo(newCenter);
					
				} else {
					sum(newCenter, aTuple);
				}
			}
			divide(newCenter, vectorList.size());

			int groupk = ((TInt) tuple.get(0)).getValue();
			key.setValue(groupk);
			for (Tuple vector : vectorList) {
				vector.copyTo(value);
				actionOutput.output(key, vector.get(0), vector.get(1),
						vector.get(2), vector.get(3),
						vector.get(4), vector.get(5));
			}

			// Add in an in-memory data structure the new centers
			newCenters.put(groupk, newCenter);
	}

		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			// Add the new center in main memory. The synchronization is
			// necessary
			// because multiple threads could access the same memory.
			synchronized (UpdateClusters.class) {
				@SuppressWarnings("unchecked")
				Map<Integer, Tuple> existingMap = (Map<Integer, Tuple>) context
						.getObjectFromCache(NEW_CENTERS);
				if (existingMap != null) {
					existingMap.putAll(newCenters);
				} else {
					context.putObjectInCache(NEW_CENTERS, newCenters);
				}
			}
			newCenters = null;
		}
	}
	
	/* Read the centers and put them in a list */
	public static class UpdateCentroids extends Action {
		public static final int S_OUTPUT_DIR = 0;
		public static final int S_DICTIONARY = 1;
		private String outputDir;
		private String dictionary;
		private Map<Integer, Integer> buckets;

		@Override
		protected void registerActionParameters(ActionConf conf) {
			conf.registerParameter(S_OUTPUT_DIR, "", null, true);
			conf.registerParameter(S_DICTIONARY, "", null, true);
		}

		@Override
		public void startProcess(ActionContext context) throws Exception {
			outputDir = getParamString(S_OUTPUT_DIR);
			dictionary = getParamString(S_DICTIONARY);
			buckets = new HashMap<Integer, Integer>();
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			buckets.put(((TInt) tuple.get(1)).getValue(),
					((TInt) tuple.get(0)).getValue());
		}

		@SuppressWarnings("unchecked")
		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			context.incrCounter("K-Means iterations", 1);
			long distance = 0;

			Map<Integer, Tuple> centersPreviousIteration = (Map<Integer, Tuple>) context
					.getObjectFromCache(CENTERS);
			Map<Integer, Tuple> localCenters = (Map<Integer, Tuple>) context
					.getObjectFromCache(NEW_CENTERS);
			List<Object[]> otherCenters = context
					.retrieveCacheObjects(NEW_CENTERS);
			Map<Integer, Tuple> centersForNextIteration = new HashMap<Integer, Tuple>();

			// Check if they are changed or not
			for (Map.Entry<Integer, Tuple> pair : localCenters.entrySet()) {
				double d = euclideanDistance(pair.getValue(),
						centersPreviousIteration.get(pair.getKey()));
				long dd = Math.round(d * 100);
				distance += dd / 100;

				centersForNextIteration.put(pair.getKey(), pair.getValue());
			}

			// Also check the other centers
			if (otherCenters != null) {
				for (Object[] oCenters : otherCenters) {
					Map<Integer, Tuple> centers = (Map<Integer, Tuple>) oCenters[0];
					for (Map.Entry<Integer, Tuple> pair : centers.entrySet()) {
						
						double d = euclideanDistance(pair.getValue(),
								centersPreviousIteration.get(pair.getKey()));
						long dd = Math.round(d * 100);
						distance += dd / 100;

						centersForNextIteration.put(pair.getKey(),
								pair.getValue());
					}
				}
			}
			System.out.println("dist");
			System.out.println(distance);
			boolean changed = distance > CONVERGENCE_THRESHOLD;
			if (changed) {
				// Update the new centers
				context.putObjectInCache(NEW_CENTERS, null);
				context.putObjectInCache(CENTERS, centersForNextIteration);
				context.broadcastCacheObjects(CENTERS, NEW_CENTERS);
			}

			int[] nodeIds = new int[buckets.size()];
			int[] bucketIds = new int[buckets.size()];
			int i = 0;
			for (Map.Entry<Integer, Integer> entry : buckets.entrySet()) {
				nodeIds[i] = entry.getValue();
				bucketIds[i] = entry.getKey();
				i++;
			}

			ActionSequence actions = new ActionSequence();
			ActionConf action = ActionFactory
					.getActionConf(ReadFromBucket.class);

			action.setParamIntArray(ReadFromBucket.IA_BUCKET_IDS, bucketIds);
			action.setParamIntArray(ReadFromBucket.IA_NODE_IDS, nodeIds);
			actions.add(action);

			if (changed) {
				// Restart the cycle
				System.out.println("restart the cycle");
				kmeans(actions, dictionary, outputDir);
			} else {
				System.out.println("end the cycle");
				// Add this split to the main branch
				action = ActionFactory.getActionConf(Split.class);
				action.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 1);
				actions.add(action);
				
				action = ActionFactory.getActionConf(WriteToFiles.class);
				action.setParamString(WriteToFiles.S_PATH, outputDir);
				actions.add(action);
				
				// Rebuild the tuples for compression
				ActionConf c = ActionFactory.getActionConf(BuildTupleForCompression.class);
				actions.add(c);
				
				compressCont(actions, dictionary, outputDir);
			}
			actionOutput.branch(actions);
		}
	}
	
	public static class BuildTupleForCompression extends Action {
		@Override
		public void process(Tuple tuple, ActionContext context, 
				ActionOutput actionOutput) throws Exception {
			Tuple outputTuple = TupleFactory.newTuple(tuple.get(1), tuple.get(2), tuple.get(3));
			actionOutput.output(outputTuple);
		}
		
	}
}
