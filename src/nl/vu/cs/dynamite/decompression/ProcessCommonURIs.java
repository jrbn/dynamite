package nl.vu.cs.dynamite.decompression;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.chains.Chain;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessCommonURIs extends Action {

	protected static Logger log = LoggerFactory
			.getLogger(ProcessCommonURIs.class);

	public static final int I_SAMPLING_THRESHOLD = 0;

	int samplingThreshold, counter;
	long currentURI;
	Set<Long> popularURIs;
	Chain newChain = new Chain();

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_SAMPLING_THRESHOLD, "I_SAMPLING_THRESHOLD", null,
				true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		samplingThreshold = getParamInt(I_SAMPLING_THRESHOLD);
		currentURI = -1;
		counter = 0;
		popularURIs = new HashSet<Long>();
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		TLong uri = (TLong) inputTuple.get(0);

		if (currentURI == -1) {
			currentURI = uri.getValue();
		} else {
			if (uri.getValue() != currentURI) {
				if (counter >= samplingThreshold) {
					popularURIs.add(currentURI);
				}
				counter = 0;
				currentURI = uri.getValue();
			}
		}

		// Count the popular URIs
		counter++;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {

		// Add the last possible one
		if (counter >= samplingThreshold) {
			popularURIs.add(currentURI);
		}

		// Broadcast map of popular URIs
		if (popularURIs.size() > 0) {
			context.putObjectInCache("popularURIs", popularURIs);
			context.broadcastCacheObjects("popularURIs");
		}
	}
}
