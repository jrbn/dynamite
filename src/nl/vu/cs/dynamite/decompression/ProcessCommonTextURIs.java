package nl.vu.cs.dynamite.decompression;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessCommonTextURIs extends Action {

	protected static Logger log = LoggerFactory
			.getLogger(ProcessCommonTextURIs.class);

	Map<Long, String> popularURIs;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		popularURIs = new HashMap<Long, String>();
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		TLong nValue = (TLong) inputTuple.get(0);
		TString tValue = (TString) inputTuple.get(1);
		popularURIs.put(nValue.getValue(), tValue.getValue());
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {

		// Broadcast map of popular URIs
		if (popularURIs.size() > 0) {
			context.putObjectInCache("popularTextURIs", popularURIs);
			context.broadcastCacheObjects("popularTextURIs");
		}
	}
}
