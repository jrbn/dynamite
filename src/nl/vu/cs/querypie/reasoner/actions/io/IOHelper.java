package nl.vu.cs.querypie.reasoner.actions.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.support.FilterHiddenFiles;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class IOHelper {

	public static TupleSet populateInMemorySetFromFile(String fileName) throws Exception {
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
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				// Parse the line
				String[] sTriple = line.split(" ");
				SimpleData[] triple = { new TLong(), new TLong(), new TLong(), new TInt() };
				((TLong) triple[0]).setValue(Long.valueOf(sTriple[0]));
				((TLong) triple[1]).setValue(Long.valueOf(sTriple[1]));
				((TLong) triple[2]).setValue(Long.valueOf(sTriple[2]));
				((TInt) triple[3]).setValue(ParamHandler.get().getLastStep());
				set.add(TupleFactory.newTuple(triple));
			}
			reader.close();
		}
		return set;
	}

}
