package nl.vu.cs.querypie.reasoner;

public class Schema {

	private static Schema instance = new Schema();

	private Schema() {
	}

	public static Schema getInstance() {
		return instance;
	}
}
