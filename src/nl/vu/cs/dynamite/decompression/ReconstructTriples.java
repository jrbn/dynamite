package nl.vu.cs.dynamite.decompression;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

public class ReconstructTriples extends Action {

	long tripleID, counter;
	byte currentPosition;

	TLong tTripleID = new TLong();
	TByte tPosition = new TByte();
	TString[] tripleOutput = new TString[3];
	Tuple outputTuple = TupleFactory.newTuple();

	@Override
	public void startProcess(ActionContext context) throws Exception {
		tripleID = -1;
		counter = currentPosition = 0;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		tTripleID = (TLong) inputTuple.get(0);
		tPosition = (TByte) inputTuple.get(1);
		tripleOutput[currentPosition++] = (TString) inputTuple.get(2);
		if (tripleID == -1) {
			tripleID = tTripleID.getValue();
			if (tripleID == -1) {
				throw new Exception("The reconstruction has failed");
			}
		} else {
			if (currentPosition == 3) {
				outputTuple.set(tripleOutput);
				output.output(outputTuple);
				tripleID = -1;
				currentPosition = 0;
				counter++;
			} else {
				// Check whether the current triple ID is the same as the
				// previous
				if (tTripleID.getValue() != tripleID) {
					throw new Exception("The reconstruction has failed");
				}
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		context.incrCounter("output triples", counter);
	}

}
