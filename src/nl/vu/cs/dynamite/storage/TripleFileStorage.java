package nl.vu.cs.dynamite.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.files.FileReader;
import nl.vu.cs.ajira.datalayer.files.FileWriter;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripleFileStorage {

	private static Logger log = LoggerFactory
			.getLogger(TripleFileStorage.class);

	public static class Writer implements FileWriter {

		private DataOutputStream os = null;

		@Override
		public void init(File file) {
			try {
				os = new DataOutputStream(new SnappyOutputStream(
						new BufferedOutputStream(new FileOutputStream(file),
								65536)));
			} catch (Exception e) {
				log.error("Error", e);
			}
		}

		@Override
		public void write(Tuple tuple) throws IOException {

			// if (log.isDebugEnabled()) {
			// if (tuple.getNElements() > 3) {
			// log.debug("Write on file the tuple " + tuple.get(0) + " "
			// + tuple.get(1) + " " + tuple.get(2) + " step="
			// + tuple.get(3));
			// } else {
			// log.debug("Write on file the tuple " + tuple.get(0) + " "
			// + tuple.get(1) + " " + tuple.get(2) + " step=" + 0);
			// }
			// }

			tuple.get(0).writeTo(os);
			tuple.get(1).writeTo(os);
			tuple.get(2).writeTo(os);
			if (tuple.getNElements() > 3) {
				tuple.get(3).writeTo(os);
			} else {
				os.writeInt(0);
			}
		}

		@Override
		public void close() throws IOException {
			if (os != null) {
				os.close();
			}
		}
	}

	public static class WriterCount implements FileWriter {

		private static Logger log = LoggerFactory.getLogger(WriterCount.class);

		private DataOutputStream os = null;

		@Override
		public void init(File file) {
			try {
				os = new DataOutputStream(new SnappyOutputStream(
						new BufferedOutputStream(new FileOutputStream(file),
								65536)));
			} catch (Exception e) {
				log.error("Error", e);
			}
		}

		@Override
		public void write(Tuple tuple) throws IOException {

			// if (log.isDebugEnabled()) {
			// log.debug("Write on file the tuple " + tuple.get(0) + " "
			// + tuple.get(1) + " " + tuple.get(2) + " step="
			// + tuple.get(3) + " C=" + tuple.get(4));
			// }

			tuple.get(0).writeTo(os);
			tuple.get(1).writeTo(os);
			tuple.get(2).writeTo(os);
			tuple.get(3).writeTo(os);
			tuple.get(4).writeTo(os); // Count
		}

		@Override
		public void close() throws IOException {
			if (os != null) {
				os.close();
			}
		}
	}

	public static class Reader implements FileReader {

		private DataInputStream is = null;
		private final TLong[] triple = { new TLong(), new TLong(), new TLong() };
		private final TInt step = new TInt();
		private final SimpleData[] signature = new SimpleData[4];

		@Override
		public void init(File file) {
			try {
				is = new DataInputStream(new SnappyInputStream(
						new BufferedInputStream(new FileInputStream(file),
								65536)));
				signature[0] = triple[0];
				signature[1] = triple[1];
				signature[2] = triple[2];
				signature[3] = step;
			} catch (Exception e) {
				log.error("Error", e);
			}
		}

		@Override
		public boolean next() {
			try {
				triple[0].setValue(is.readLong());
				triple[1].setValue(is.readLong());
				triple[2].setValue(is.readLong());
				step.setValue(is.readInt());
				return true;
			} catch (Exception e) {
			}
			return false;
		}

		@Override
		public void getTuple(Tuple tuple) {
			tuple.set(signature);
		}

		@Override
		public void close() {
			try {
				if (is != null) {
					is.close();
				}
			} catch (Exception e) {
				log.error("error", e);
			}
		}
	}

	public static class ReaderCount implements FileReader {

		private static Logger log = LoggerFactory.getLogger(ReaderCount.class);

		private DataInputStream is = null;
		private final TLong[] triple = { new TLong(), new TLong(), new TLong() };
		private final TInt step = new TInt();
		private final TInt count = new TInt();
		private final SimpleData[] signature = new SimpleData[5];

		// private File currentFile;

		@Override
		public void init(File file) {
			// currentFile = file;
			try {
				is = new DataInputStream(new SnappyInputStream(
						new BufferedInputStream(new FileInputStream(file),
								65536)));
				signature[0] = triple[0];
				signature[1] = triple[1];
				signature[2] = triple[2];
				signature[3] = step;
				signature[4] = count;
			} catch (Exception e) {
				log.error("Error", e);
			}
		}

		@Override
		public boolean next() {
			try {
				triple[0].setValue(is.readLong());
				triple[1].setValue(is.readLong());
				triple[2].setValue(is.readLong());
				step.setValue(is.readInt());
				count.setValue(is.readInt());
				return true;
			} catch (Exception e) {
			}
			return false;
		}

		@Override
		public void getTuple(Tuple tuple) {
			tuple.set(signature);
		}

		@Override
		public void close() {
			try {
				if (is != null) {
					is.close();
				}
			} catch (Exception e) {
				log.error("error", e);
			}
		}
	}

}
