package nl.vu.cs.querypie.reasoner.actions.io;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.support.FilterHiddenFiles;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

import org.iq80.snappy.SnappyInputStream;

public class IOHelper {

	public static TupleSet populateInMemorySetFromFile(String fileName)
			throws Exception {
		TupleSet set = new TupleSetImpl();
		List<File> files = new ArrayList<File>();
		File fInput = new File(fileName);
		if (fInput.isDirectory()) {
			for (File child : fInput.listFiles(new FilterHiddenFiles()))
				files.add(child);
		} else {
			files.add(fInput);
		}
		for (File file : files) {
			DataInputStream is = null;
			try {
				is = new DataInputStream(new SnappyInputStream(
						new FileInputStream(file)));
				while (true) {
					SimpleData[] triple = { new TLong(), new TLong(),
							new TLong(), new TInt() };
					((TLong) triple[0]).setValue(is.readLong());
					((TLong) triple[1]).setValue(is.readLong());
					((TLong) triple[2]).setValue(is.readLong());
					is.readInt(); // Need to read the input
					((TInt) triple[3]).setValue(ParamHandler.get()
							.getLastStep());
					set.add(TupleFactory.newTuple(triple));
				}
			} catch (EOFException e) {
			} catch (Exception e) {
			} finally {
				if (is != null)
					is.close();
			}
		}
		return set;
	}
}
