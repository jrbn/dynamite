package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionController;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.buckets.TupleSerializer;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.Query;
import nl.vu.cs.ajira.utils.Consts;

public class ReadFromBtree extends Action {

	public static final int TUPLE = 0;
	public static final int PARALLEL_TASKS = 1;

	private boolean first;
	private int tasks;
	private TupleSerializer tuple = new TupleSerializer(TupleFactory.newTuple(
			new TLong(), new TLong(), new TLong()));

	public static class CustomProcessor extends ActionConf.Configurator {

		@Override
		public void setupAction(Query query, Object[] params,
				ActionController controller, ActionContext context) {
			// Add the input tuple
			TupleSerializer tuple = (TupleSerializer) params[TUPLE];
			query.setInputLayer(Consts.DEFAULT_INPUT_LAYER_ID);

			Tuple t = tuple.getTuple();

			if ((int) params[PARALLEL_TASKS] > 1) {
				SimpleData[] newTuple = new SimpleData[5];
				newTuple[0] = t.get(0);
				newTuple[1] = t.get(1);
				newTuple[2] = t.get(2);
				newTuple[3] = new TInt(0);
				newTuple[4] = new TInt((int) params[PARALLEL_TASKS]);
				t.set(newTuple);
			} else {
				controller.doNotAddCurrentAction();
			}

			query.setInputTuple(t);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(TUPLE, "tuple", null, true);
		conf.registerParameter(PARALLEL_TASKS, "parallel tasks", 1, false);
		conf.registerCustomConfigurator(CustomProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		first = true;
		tasks = getParamInt(PARALLEL_TASKS);
		getParamWritable(tuple, TUPLE);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (first) {
			first = false;
			for (int i = 1; i < tasks; ++i) {
				ActionConf c = ActionFactory
						.getActionConf(QueryInputLayer.class);
				c.setParamInt(QueryInputLayer.INPUT_LAYER,
						Consts.DEFAULT_INPUT_LAYER_ID);

				Tuple t = this.tuple.getTuple();
				SimpleData[] newTuple = new SimpleData[5];
				newTuple[0] = t.get(0);
				newTuple[1] = t.get(1);
				newTuple[2] = t.get(2);
				newTuple[3] = new TInt(i);
				newTuple[4] = new TInt(tasks);
				t.set(newTuple);

				c.setParamWritable(QueryInputLayer.TUPLE, this.tuple);
				actionOutput.branch(c);
			}
		}
		actionOutput.output(tuple);
	}
}
