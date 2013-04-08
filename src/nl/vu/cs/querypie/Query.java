package nl.vu.cs.querypie;

import java.io.BufferedReader;
import java.io.FileReader;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.Project;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.io.AppendFileWriter;
import nl.vu.cs.querypie.reasoner.actions.io.ReadFromBtree;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query {
	static final Logger log = LoggerFactory.getLogger(Query.class);

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage: Query <KB_dir> <query file> <output file>");
			return;
		}
		try {
			Ajira arch = new Ajira();
			initAjira(args[0], arch);
			arch.startup();

			String inputFile = args[1];
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			String query;
			while ((query = reader.readLine()) != null) {
				// Query the knowledge base
				ActionSequence as = new ActionSequence();

				// Parse the query
				ActionConf c = ActionFactory.getActionConf(ReadFromBtree.class);
				long[] q = parseQuery(query);
				c.setParamWritable(ReadFromBtree.TUPLE, new nl.vu.cs.ajira.actions.support.Query(new TLong(q[0]), new TLong(q[1]), new TLong(q[2])));
				as.add(c);

				// Collect the results to one node
				c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS, TLong.class.getName(), TLong.class.getName(), TLong.class.getName(), TInt.class.getName());
				as.add(c);

				// Keep only the first three fields
				c = ActionFactory.getActionConf(Project.class);
				c.setParamByteArray(Project.BA_FIELDS, (byte) 0, (byte) 1, (byte) 2);
				as.add(c);

				// Write output to file
				c = ActionFactory.getActionConf(WriteToFiles.class);
				c.setParamString(WriteToFiles.S_CUSTOM_WRITER, AppendFileWriter.class.getName());
				c.setParamString(WriteToFiles.S_OUTPUT_DIR, args[2]);
				as.add(c);

				Job job = new Job();
				job.setActions(as);
				Submission s = arch.waitForCompletion(job);
				s.printStatistics();
			}
			arch.shutdown();
			reader.close();
		} catch (Exception e) {
			log.error("Error in the execution", e);
		}
	}

	private static void initAjira(String kbDir, Ajira arch) {
		Configuration conf = arch.getConfiguration();
		conf.set(Consts.STORAGE_IMPL, BerkeleydbLayer.class.getName());
		conf.set(BerkeleydbLayer.DB_INPUT, kbDir);
		conf.setInt(Consts.N_PROC_THREADS, 4);
	}

	private static long[] parseQuery(String q) {
		String[] terms = q.split(" ");
		long[] output = new long[3];
		output[0] = Long.valueOf(terms[0]);
		output[1] = Long.valueOf(terms[1]);
		output[2] = Long.valueOf(terms[2]);
		return output;
	}
}
