package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;

class WriteDerivationsBtree extends Action {

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private boolean newValue;
	private final byte[] triple = new byte[24];
	private final byte[] meta = new byte[4];
	private long dupCount, newCount;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		in = ReasoningContext.getInstance().getKB();
		spo = in.openWritingSession(DBType.SPO);
		sop = in.openWritingSession(DBType.SOP);
		pso = in.openWritingSession(DBType.PSO);
		pos = in.openWritingSession(DBType.POS);
		osp = in.openWritingSession(DBType.OSP);
		ops = in.openWritingSession(DBType.OPS);
		newValue = false;
		newCount = dupCount = 0;
	}

	private void encode(long v1, long v2, long v3) {
		Utils.encodeLong(triple, 0, v1);
		Utils.encodeLong(triple, 8, v2);
		Utils.encodeLong(triple, 16, v3);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		TLong s = (TLong) tuple.get(0);
		TLong p = (TLong) tuple.get(1);
		TLong o = (TLong) tuple.get(2);
		TInt step = (TInt) tuple.get(3);
		encode(s.getValue(), p.getValue(), o.getValue());

		// Set the iteration count the triple was derived from
		Utils.encodeInt(meta, 0, step.getValue());

		if (spo.write(triple, meta) == WritingSession.SUCCESS) {
			// Add it also in the other permutations
			encode(s.getValue(), o.getValue(), p.getValue());
			sop.writeKey(triple);

			encode(p.getValue(), o.getValue(), s.getValue());
			pos.writeKey(triple);

			encode(p.getValue(), s.getValue(), o.getValue());
			pso.writeKey(triple);

			encode(o.getValue(), p.getValue(), s.getValue());
			ops.writeKey(triple);

			encode(o.getValue(), s.getValue(), p.getValue());
			osp.writeKey(triple);

			if (!newValue) {
				actionOutput.output(tuple);
				newValue = true;
			}
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
