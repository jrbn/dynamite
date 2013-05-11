package nl.vu.cs.dynamite;

import java.io.File;

import nl.vu.cs.ajira.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BenchmarkStorage {

	static final Logger log = LoggerFactory.getLogger(BenchmarkStorage.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		File f = new File(args[0]);
		if (f.exists()) {
			f.delete();
		}
		f.mkdir();

		// Start database
		Environment env = null;
		Database db = null;
		try {
			// Open the environment
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setCacheSize(1024 * 1024 * 1024); // 512MB
			env = new Environment(new File(args[0]), envConfig);

			// Open the database. Create it if it does not already exist.
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			dbConfig.setDeferredWrite(true);
			db = env.openDatabase(null, "sample", dbConfig);
		} catch (DatabaseException dbe) {
			log.error("Error", dbe);
		}

		long counter = 0;
		final DatabaseEntry value = new DatabaseEntry(new byte[0]);
		final DatabaseEntry key = new DatabaseEntry();

		// Write all the records
		long time = System.currentTimeMillis();
		byte[] k = new byte[8];

		while (counter++ < 100000000) {
			Utils.encodeLong(k, 0, counter);
			key.setData(k);
			db.put(null, key, value);
			if (counter % 1000000 == 0) {
				System.out.println("Records: " + counter / 1000
						+ "K time (s): " + (System.currentTimeMillis() - time)
						/ 1000);
			}
		}

		// Close the database
		try {
			if (db != null) {
				db.close();
			}

			if (env != null) {
				env.close();
			}
		} catch (DatabaseException dbe) {
			log.error("Error", dbe);
		}

	}
}
