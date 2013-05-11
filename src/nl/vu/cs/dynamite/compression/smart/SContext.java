package nl.vu.cs.dynamite.compression.smart;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class SContext {

	private Multimap<String, String> superclasses = HashMultimap.create();
	private Multimap<String, String> superproperties = HashMultimap.create();

	public void addSuperclass(String s, String o) {
		superclasses.put(s, o);
	}

	public void addSuperproperty(String s, String o) {
		superproperties.put(s, o);
	}

	public Collection<String> getSuperclasses(String s) {
		return superclasses.get(s);
	}

	public Collection<String> getSuperproperties(String s) {
		return superproperties.get(s);
	}
}
