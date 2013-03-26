package nl.vu.cs.querypie.reasoner.actions;

import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class RemoveDerivationsBtree extends Action {

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private long removedTriples;
	private long notRemovedTriples;
	private byte[] key = new byte[24];

	@Override
	public void startProcess(ActionContext context) throws Exception {
		in = ReasoningContext.getInstance().getKB();
		spo = in.openWritingSession(DBType.SPO);
		sop = in.openWritingSession(DBType.SOP);
		pso = in.openWritingSession(DBType.PSO);
		pos = in.openWritingSession(DBType.POS);
		osp = in.openWritingSession(DBType.OSP);
		ops = in.openWritingSession(DBType.OPS);
		removedTriples = notRemovedTriples = 0;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// Remove the content of the derivation from the btrees

		TupleStepMap map = (TupleStepMap) context
				.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);

		for (Map.Entry<Tuple, Integer> entry : map.entrySet()) {
			TLong s = (TLong) entry.getKey().get(0);
			TLong p = (TLong) entry.getKey().get(1);
			TLong o = (TLong) entry.getKey().get(2);

			encode(s.getValue(), p.getValue(), o.getValue());
			boolean removed = spo.decreaseOrRemove(key);

			encode(s.getValue(), o.getValue(), p.getValue());
			sop.decreaseOrRemove(key);

			encode(p.getValue(), o.getValue(), s.getValue());
			pos.decreaseOrRemove(key);

			encode(p.getValue(), s.getValue(), o.getValue());
			pso.decreaseOrRemove(key);

			encode(o.getValue(), s.getValue(), p.getValue());
			osp.decreaseOrRemove(key);

			encode(o.getValue(), p.getValue(), s.getValue());
			ops.decreaseOrRemove(key);

			if (removed) {
				removedTriples++;
			} else {
				notRemovedTriples++;
			}
		}

	}

	private void encode(long v1, long v2, long v3) {
		Utils.encodeLong(key, 0, v1);
		Utils.encodeLong(key, 8, v2);
		Utils.encodeLong(key, 16, v3);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		spo.close();
		sop.close();
		ops.close();
		osp.close();
		pos.close();
		pso.close();
		context.incrCounter("Removed triples", removedTriples);
		context.incrCounter("Triples that could still be derived",
				notRemovedTriples);
	}
}
