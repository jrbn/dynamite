package nl.vu.cs.dynamite.storage;

import java.io.IOException;

import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.files.DefaultFileReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripleReader extends DefaultFileReader {

	static final Logger log = LoggerFactory.getLogger(TripleReader.class);

	TLong[] triple = { new TLong(), new TLong(), new TLong() };

	@Override
	public boolean next() throws IOException {
		String t = reader.readLine();
		if (t == null) {
			reader.close();
			return false;
		}
		// Parse the textual line
		String[] terms = t.split(" ");
		triple[0].setValue(Long.valueOf(terms[0]));
		triple[1].setValue(Long.valueOf(terms[1]));
		triple[2].setValue(Long.valueOf(terms[2]));
		return true;
	}

	@Override
	public void getTuple(Tuple tuple) {
		tuple.set(triple);
	}

}
