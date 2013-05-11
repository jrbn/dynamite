package nl.vu.cs.dynamite.reasoner.actions.io;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionController;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.dynamite.storage.Pattern;

public class ReadFromBtree extends Action {
	public static void addToChain(Pattern pattern, ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(ReadFromBtree.class);
		Query query = new Query(new TLong(pattern.getTerm(0).getValue()), new TLong(pattern.getTerm(1).getValue()), new TLong(pattern.getTerm(2).getValue()));
		a.setParamWritable(ReadFromBtree.W_TUPLE, query);
		actions.add(a);
	}

	public static final int W_TUPLE = 0;
	public static final int I_PARALLEL_TASKS = 1;

	private boolean first;
	private int tasks;
	private final Query query = new Query(TupleFactory.newTuple(new TLong(), new TLong(), new TLong()));

	public static class CustomProcessor extends ActionConf.Configurator {

		@Override
		public void setupAction(InputQuery query, Object[] params, ActionController controller, ActionContext context) {
			// Add the input tuple
			Query tuple = null;
			if (params[W_TUPLE] instanceof byte[]) {
				tuple = new Query();
				try {
					tuple.readFrom(new BDataInput((byte[]) params[W_TUPLE]));
				} catch (Exception e) {
					log.error("Error in reading", e);
				}
			} else {
				tuple = (Query) params[W_TUPLE];
			}
			query.setInputLayer(InputLayer.DEFAULT_LAYER);

			Tuple t = tuple.getTuple();

			if ((Integer) params[I_PARALLEL_TASKS] > 1) {
				SimpleData[] newTuple = new SimpleData[5];
				newTuple[0] = t.get(0);
				newTuple[1] = t.get(1);
				newTuple[2] = t.get(2);
				newTuple[3] = new TInt(0);
				newTuple[4] = new TInt((Integer) params[I_PARALLEL_TASKS]);
				t.set(newTuple);
			} else {
				controller.doNotAddCurrentAction();
			}

			query.setQuery(tuple);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(W_TUPLE, "W_TUPLE", null, true);
		conf.registerParameter(I_PARALLEL_TASKS, "I_PARALLEL_TASKS", 1, false);
		conf.registerCustomConfigurator(new CustomProcessor());
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		first = true;
		tasks = getParamInt(I_PARALLEL_TASKS);
		getParamWritable(query, W_TUPLE);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		if (first) {
			first = false;
			for (int i = 1; i < tasks; ++i) {
				ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
				c.setParamString(QueryInputLayer.S_INPUTLAYER, "DEFAULT");

				Tuple t = query.getTuple();
				SimpleData[] newTuple = new SimpleData[5];
				newTuple[0] = t.get(0);
				newTuple[1] = t.get(1);
				newTuple[2] = t.get(2);
				newTuple[3] = new TInt(i);
				newTuple[4] = new TInt(tasks);
				t.set(newTuple);

				c.setParamWritable(QueryInputLayer.W_QUERY, query);
				c.setParamStringArray(QueryInputLayer.SA_SIGNATURE_QUERY, t.getSignature());
				actionOutput.branch(new ActionSequence(c));
			}
		}
		actionOutput.output(tuple);
	}
}
