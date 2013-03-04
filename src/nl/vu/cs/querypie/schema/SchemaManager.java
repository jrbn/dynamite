package nl.vu.cs.querypie.schema;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.Tuples;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

public class SchemaManager {

	private BerkeleydbLayer kb;

	public SchemaManager(Ajira arch) {
		kb = (BerkeleydbLayer) arch.getContext().getInputLayer(
				Consts.DEFAULT_INPUT_LAYER_ID);
	}

	public Tuples getTuples(Pattern[] patterns) {
		// TODO Auto-generated method stub
		return null;
	}
}
