package nl.vu.cs.querypie.reasoner.actions.io;

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
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;

public class WriteDerivationsBtree extends Action {
	public static void addToChain(boolean forceStep, int step, ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(WriteDerivationsBtree.class);
		c.setParamInt(WriteDerivationsBtree.I_STEP, step);
		c.setParamBoolean(WriteDerivationsBtree.B_FORCE_STEP, forceStep);
		actions.add(c);
	}

	public static final int I_STEP = 0;
	public static final int B_FORCE_STEP = 1;

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private boolean newValue;
	private final byte[] triple = new byte[24];
	private final byte[] meta = new byte[8];
	private long dupCount, newCount;
	private int step;
	private boolean forceStep, considerCount;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", -1, false);
		conf.registerParameter(B_FORCE_STEP, "force step", null, true);
	}

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
		considerCount = ParamHandler.get().isUsingCount();
		forceStep = getParamBoolean(B_FORCE_STEP);
		if (forceStep) {
			step = getParamInt(I_STEP);
			Utils.encodeInt(meta, 0, step);
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		TLong s = (TLong) tuple.get(0);
		TLong p = (TLong) tuple.get(1);
		TLong o = (TLong) tuple.get(2);

		if (!forceStep) {
			int idx = 4;
			if (!considerCount)
				idx--;
			TInt step = (TInt) tuple.get(idx);
			Utils.encodeInt(meta, 0, step.getValue());
		}

		encode(s.getValue(), p.getValue(), o.getValue());

		boolean newTuple = false;
		if (considerCount) {
			TInt count = (TInt) tuple.get(3);
			int c = count.getValue();
			newTuple = spo.write(triple, meta, c, false) == WritingSession.SUCCESS;

			// Add it also in the other permutations
			encode(s.getValue(), o.getValue(), p.getValue());
			sop.write(triple, meta, c, newTuple);

			encode(p.getValue(), o.getValue(), s.getValue());
			pos.write(triple, meta, c, newTuple);

			encode(p.getValue(), s.getValue(), o.getValue());
			pso.write(triple, meta, c, newTuple);

			encode(o.getValue(), p.getValue(), s.getValue());
			ops.write(triple, meta, c, newTuple);

			encode(o.getValue(), s.getValue(), p.getValue());
			osp.write(triple, meta, c, newTuple);
		} else {
			newTuple = spo.write(triple, meta) == WritingSession.SUCCESS;

			// Add it also in the other permutations
			encode(s.getValue(), o.getValue(), p.getValue());
			sop.write(triple, meta);

			encode(p.getValue(), o.getValue(), s.getValue());
			pos.write(triple, meta);

			encode(p.getValue(), s.getValue(), o.getValue());
			pso.write(triple, meta);

			encode(o.getValue(), p.getValue(), s.getValue());
			ops.write(triple, meta);

			encode(o.getValue(), s.getValue(), p.getValue());
			osp.write(triple, meta);
		}

		if (newTuple) {
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
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		spo.close();
		sop.close();
		ops.close();
		osp.close();
		pos.close();
		pso.close();
		context.incrCounter("Derived duplicates", dupCount);
		context.incrCounter("New Derivations", newCount);
	}

	private void encode(long v1, long v2, long v3) {
		Utils.encodeLong(triple, 0, v1);
		Utils.encodeLong(triple, 8, v2);
		Utils.encodeLong(triple, 16, v3);
	}

}
