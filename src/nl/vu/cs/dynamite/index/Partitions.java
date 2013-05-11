package nl.vu.cs.dynamite.index;

import java.io.Serializable;

import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.dynamite.storage.DBType;

public class Partitions implements Serializable {

	public static final DBType[] partition_labels = { DBType.SPO, DBType.SOP,
			DBType.POS, DBType.PSO, DBType.OPS, DBType.OSP };
	public static final TByte[] partition_ids = { new TByte(0), new TByte(1),
			new TByte(2), new TByte(3), new TByte(4), new TByte(5) };

	private static final long serialVersionUID = 7787473899807628403L;

	private final TLong[][][] list = new TLong[6][][];

	public TLong[][] get(int value) {
		return list[value];
	}

	public void add(int id, TLong[][] list) {
		this.list[id] = list;
	}

}
