package nl.vu.cs.querypie.reasoner.support;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

class DebuggingUtils {

	private static final String separator = " ---------------- \n";

	static void printDerivations(BerkeleydbLayer db, ActionContext context) {
		String debugString = printDerivationsToString(db, context);
		System.out.println(debugString);
	}

	static void printDerivationsToFile(String filename, BerkeleydbLayer db, ActionContext context) {
		try {
			FileOutputStream fos = new FileOutputStream(new File(filename), true);
			String debugString = printDerivationsToString(db, context);
			fos.write(debugString.getBytes());
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String printDerivationsToString(BerkeleydbLayer db, ActionContext context) {
		String result = separator;
		Tuple query = TupleFactory.newTuple(new TLong(-1), new TLong(-1), new TLong(-1));
		TupleIterator it = db.getIterator(query, context);
		Tuple tuple = TupleFactory.newTuple();
		int count = 0;
		try {
			while (it.isReady() && it.nextTuple()) {
				result = result.concat(String.valueOf(++count));
				result = result.concat("\t");
				it.getTuple(tuple);
				for (int i = 0; i < 3; ++i) {
					long val = ((TLong) tuple.get(i)).getValue();
					String stringVal = String.valueOf(val);
					switch (stringVal) {
					case "0":
						stringVal = "type";
						break;
					case "1":
						stringVal = "property";
						break;
					case "2":
						stringVal = "range";
						break;
					case "3":
						stringVal = "domain";
						break;
					case "4":
						stringVal = "subproperty";
						break;
					case "5":
						stringVal = "subclass";
						break;
					case "20":
						stringVal = "literal";
						break;
					case "22":
						stringVal = "datatype";
						break;
					case "23":
						stringVal = "class";
						break;
					case "24":
						stringVal = "resource";
						break;
					}
					if (i != 0) {
						result = result.concat("\t");
					}
					result = result.concat(stringVal);
				}
				for (int i = 3; i < tuple.getNElements(); ++i) {
					int val = ((TInt) tuple.get(i)).getValue();
					String stringVal = String.valueOf(val);
					result = result.concat("\t");
					result = result.concat(stringVal);
				}
				result = result.concat("\n");
			}
			result = result.concat(separator);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
