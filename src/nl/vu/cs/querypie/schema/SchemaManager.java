package nl.vu.cs.querypie.schema;

import java.util.Arrays;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.sets.Tuples;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaManager {

	static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

	private BerkeleydbLayer kb;

	public SchemaManager(Ajira arch) {
		kb = (BerkeleydbLayer) arch.getContext().getInputLayer(
				Consts.DEFAULT_INPUT_LAYER_ID);
	}

	public Tuples getTuples(Pattern[] patterns) {

		// Retrieve the triples for each pattern
		int i = 0;
		long[][] tuples = new long[patterns.length][];
		int[][] pos_vars = new int[patterns.length][];

		TLong[] query = new TLong[3];
		for (Pattern p : patterns) {

			// Get the triples
			int nvars = 0;
			int[] posToCopy = new int[3];
			for (int j = 0; j < 3; ++j) {
				if (p.getTerm(j).getName() != null) {
					query[j] = new TLong(-1);
					posToCopy[nvars++] = j;
				} else {
					query[j] = new TLong(p.getTerm(j).getValue());
				}
			}
			pos_vars[i] = Arrays.copyOf(posToCopy, nvars);
			Tuple t = TupleFactory.newTuple(query);
			TupleIterator itr = kb.getIterator(t, null);

			// Copy the bindings on a new data structure
			long[] rawValues = new long[1];
			int counter = 0;
			try {
				while (itr.isReady() && itr.nextTuple()) {
					itr.getTuple(t);
					if (rawValues.length <= counter + nvars) {
						rawValues = Arrays.copyOf(rawValues,
								rawValues.length * 2);
					}

					for (int m = 0; m < nvars; ++m) {
						rawValues[counter++] = query[posToCopy[m]].getValue();
					}

				}
			} catch (Exception e) {
				log.error("Error", e);
			}

			tuples[i++] = Arrays.copyOf(rawValues, counter);
		}

		// Join the bindings using an hash join
		Tuples output = new Tuples(pos_vars[0].length, tuples[0]);
		if (patterns.length > 1) {
			// TODO: Join with the sequent patterns
		}
		return output;
	}
}
