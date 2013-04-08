package nl.vu.cs.querypie.reasoner.actions.io;

import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.support.Consts;
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class RemoveDerivationsBtree extends Action {
	public static void addToChain(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(RemoveDerivationsBtree.class);
		actions.add(c);
	}

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private long removedTriples;
	private long notRemovedTriples;
	private final byte[] key = new byte[24];

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
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		// Remove the content of the derivation from the BTrees
		Object obj = context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		Set<Tuple> tuplesToRemove = null;
		if (obj instanceof TupleSet) {
			tuplesToRemove = ((TupleSet) obj);
		} else if (obj instanceof TupleStepMap) {
			tuplesToRemove = ((TupleStepMap) obj).keySet();
		} else {
			throw new Exception("Unknown in memory implementation");
		}
		removeAllTuplesInSet(tuplesToRemove);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		spo.close();
		sop.close();
		ops.close();
		osp.close();
		pos.close();
		pso.close();
		context.incrCounter("Removed triples", removedTriples);
		context.incrCounter("Triples that could still be derived", notRemovedTriples);
	}

	private void removeAllTuplesInSet(Set<Tuple> set) {
		for (Tuple tuple : set) {
			TLong s = (TLong) tuple.get(0);
			TLong p = (TLong) tuple.get(1);
			TLong o = (TLong) tuple.get(2);

			boolean removed = false;
			if (ParamHandler.get().isUsingCount()) {
				encode(s.getValue(), p.getValue(), o.getValue());
				removed = spo.decreaseOrRemove(key);

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
			} else {
				removed = true;
				encode(s.getValue(), p.getValue(), o.getValue());
				spo.remove(key);

				encode(s.getValue(), o.getValue(), p.getValue());
				sop.remove(key);

				encode(p.getValue(), o.getValue(), s.getValue());
				pos.remove(key);

				encode(p.getValue(), s.getValue(), o.getValue());
				pso.remove(key);

				encode(o.getValue(), s.getValue(), p.getValue());
				osp.remove(key);

				encode(o.getValue(), p.getValue(), s.getValue());
				ops.remove(key);
			}

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
}
