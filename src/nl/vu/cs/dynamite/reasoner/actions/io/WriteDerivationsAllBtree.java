package nl.vu.cs.dynamite.reasoner.actions.io;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.ReasoningContext;
import nl.vu.cs.dynamite.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;

public class WriteDerivationsAllBtree extends Action {
	public static void addToChain(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(WriteDerivationsAllBtree.class);
		actions.add(c);
	}

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private final byte[] triple = new byte[24];
	private byte[] meta;
	private long dupCount, newCount;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		in = (BTreeInterface) ReasoningContext.getInstance().getKB();
		spo = in.openWritingSession(context, DBType.SPO);
		sop = in.openWritingSession(context, DBType.SOP);
		pso = in.openWritingSession(context, DBType.PSO);
		pos = in.openWritingSession(context, DBType.POS);
		osp = in.openWritingSession(context, DBType.OSP);
		ops = in.openWritingSession(context, DBType.OPS);
		newCount = dupCount = 0;
		if (ParamHandler.get().isUsingCount()) {
			meta = new byte[8];
		} else {
			meta = new byte[4];
		}

	}

	private int encode(long l1, long l2, long l3) {
		return in.encode(triple, l1, l2, l3);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		TLong s = (TLong) tuple.get(0);
		TLong p = (TLong) tuple.get(1);
		TLong o = (TLong) tuple.get(2);
		TInt step = (TInt) tuple.get(3);

		int keyLen = encode(s.getValue(), p.getValue(), o.getValue());
		Utils.encodeInt(meta, 0, step.getValue());

		boolean newTuple = false;
		if (ParamHandler.get().isUsingCount()) {
			TInt count = (TInt) tuple.get(4);
			int c = count.getValue();
			newTuple = spo.writeWithCount(triple, keyLen, meta, c, false) == WritingSession.SUCCESS;

			// Add it also in the other permutations
			keyLen = encode(s.getValue(), o.getValue(), p.getValue());
			sop.writeWithCount(triple, keyLen, meta, c, newTuple);

			keyLen = encode(p.getValue(), o.getValue(), s.getValue());
			pos.writeWithCount(triple, keyLen, meta, c, newTuple);

			keyLen = encode(p.getValue(), s.getValue(), o.getValue());
			pso.writeWithCount(triple, keyLen, meta, c, newTuple);

			keyLen = encode(o.getValue(), p.getValue(), s.getValue());
			ops.writeWithCount(triple, keyLen, meta, c, newTuple);

			keyLen = encode(o.getValue(), s.getValue(), p.getValue());
			osp.writeWithCount(triple, keyLen, meta, c, newTuple);
		} else {
			newTuple = spo.write(triple, keyLen, meta) == WritingSession.SUCCESS;

			// Add it also in the other permutations
			keyLen = encode(s.getValue(), o.getValue(), p.getValue());
			sop.write(triple, keyLen, meta);

			keyLen = encode(p.getValue(), o.getValue(), s.getValue());
			pos.write(triple, keyLen, meta);

			keyLen = encode(p.getValue(), s.getValue(), o.getValue());
			pso.write(triple, keyLen, meta);

			keyLen = encode(o.getValue(), p.getValue(), s.getValue());
			ops.write(triple, keyLen, meta);

			keyLen = encode(o.getValue(), s.getValue(), p.getValue());
			osp.write(triple, keyLen, meta);
		}

		if (newTuple) {
			actionOutput.output(tuple);
			newCount++;
		} else {
			dupCount++;
		}
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
		context.incrCounter("Derived duplicates", dupCount);
		context.incrCounter("New Derivations", newCount);
	}
}
