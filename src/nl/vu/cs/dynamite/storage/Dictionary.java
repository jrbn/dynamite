package nl.vu.cs.dynamite.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.files.FileReader;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.xerial.snappy.SnappyInputStream;
// import org.xerial.snappy.SnappyOutputStream;

public class Dictionary {

	public static class Pair {
		public String value;
		public long key;

		Pair(long key, String value) {
			this.key = key;
			this.value = value;
		}
	}

	public static class FilterOnlyDictionaryFiles implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {
			return !name.startsWith(".") && !name.startsWith("_")
					&& !name.endsWith(".txt");
		}

	}

	public static class PairIterator implements Iterator<Pair> {

		private Iterator<File> itr = null;
		private Reader currentFile = null;
		private final Tuple tuple = TupleFactory.newTuple(new TLong(),
				new TString());;

		public PairIterator(Collection<File> files) {
			itr = files.iterator();
			currentFile = null;
		}

		@Override
		public boolean hasNext() {
			if (!currentFile.next()) {
				try {
					if (!itr.hasNext()) {
						return false;
					}
					File newFile = itr.next();
					currentFile = new Reader();
					currentFile.init(newFile);
				} catch (Exception e) {
					return false;
				}
			}
			return true;
		}

		@Override
		public Pair next() {
			try {
				currentFile.getTuple(tuple);
				return new Pair(((TLong) tuple.get(0)).getValue(),
						((TString) tuple.get(1)).getValue());
			} catch (Exception e) {
			}
			return null;
		}

		@Override
		public void remove() {
		}

	}

	public static class Reader implements FileReader {

		final static Logger log = LoggerFactory.getLogger(Reader.class);

		DataInputStream stream;

		TLong number = new TLong();
		TString txt = new TString();

		@Override
		public void init(File file) {
			try {
				stream = new DataInputStream(new SnappyInputStream(
						new FileInputStream(file)));
			} catch (Exception e) {
				log.error("", e);
			}
		}

		@Override
		public boolean next() {
			try {
				number.setValue(stream.readLong());
				txt.setValue(stream.readUTF());
				return true;
			} catch (java.io.EOFException e2) {
			} catch (IOException e1) {
				log.error("Error", e1);
			}
			return false;
		}

		@Override
		public void getTuple(Tuple tuple) {
			tuple.set(number, txt);
		}

		@Override
		public void close() {
			try {
				stream.close();
			} catch (Exception e) {
				log.error("", e);
			}
		}
	}

	private DataOutputStream stream;

	public void openDictionary(String dir, String prefix) throws Exception {
		// Create a new file. Look for the highest number
		int counter = 0;
		File fDir = new File(dir);
		if (!fDir.exists() || !fDir.isDirectory()) {
			throw new Exception(dir + " is not a directory");
		} else {
			File[] files = fDir.listFiles();
			for (File child : files) {
				if (child.getName().indexOf('-') != -1) {
					String name = child.getName();
					int c = Integer
							.parseInt(name.substring(name.indexOf('-') + 1));
					if (c > counter) {
						counter = c;
					}
				}
			}
		}
		openDictionary(dir, prefix, ++counter);
	}

	public void openDictionary(String dir, String prefix, long unique_prefix)
			throws FileNotFoundException, IOException {
		String filename = prefix + "-";
		NumberFormat f = NumberFormat.getInstance();
		f.setMinimumIntegerDigits(5);
		f.setGroupingUsed(false);
		filename += f.format(unique_prefix);
		
		File fDir = new File(dir);
		if (!fDir.exists() || !fDir.isDirectory()) {
			fDir.mkdirs();
		}

		// Write the URIs in a file in the output dir
		stream = new DataOutputStream(new SnappyOutputStream(
				new FileOutputStream(new File(dir, filename))));
	}

	public void writeNewTerm(long numValue, String txtValue) throws IOException {
		stream.writeLong(numValue);
		stream.writeUTF(txtValue);
	}

	public void closeDictionary() throws IOException {
		stream.close();
	}

	public static Iterator<Pair> readAllPairs(String dir, String prefix) {
		// Read all the files from the given directory
		File file = new File(dir);
		if (file.exists()) {
			List<File> list = new ArrayList<File>();
			for (File child : file.listFiles()) {
				if (child.getName().startsWith(prefix)) {
					list.add(child);
				}
			}
		}
		return null;

	}
}
