package com.rationalworks.data.processor.collection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MultiKeyMap<A, B, C> implements IMultiKeyMap<A, B, C> {

	Map<InnerKey, C> parentMap = new HashMap<InnerKey, C>();

	@SuppressWarnings("unchecked")
	public void put(A key1, B key2, C value) {
		parentMap.put(new MultiKeyMap.InnerKey(key1, key2), value);

	}

	public C get(A key1, B key2) {

		return parentMap.get(new MultiKeyMap.InnerKey(key1, key2));
	}

	class InnerKey {
		private A key1;
		private B key2;

		public InnerKey(A key1, B key2) {
			this.key1 = key1;
			this.key2 = key2;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof MultiKeyMap.InnerKey) {
				MultiKeyMap.InnerKey keyPair = (MultiKeyMap.InnerKey) o;
				return key1.equals(keyPair.key1) && key2.equals(keyPair.key2);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return key1.hashCode() * key2.hashCode();
		}

	}

	public void putAll(MultiKeyMap<A, B, C> tableMetadata) {
		// TODO Auto-generated method stub
		Set<MultiKeyMap<A, B, C>.InnerKey> keySet = tableMetadata.keySet();
		Iterator<MultiKeyMap<A, B, C>.InnerKey> setItr = keySet.iterator();
		while (setItr.hasNext()) {
			MultiKeyMap<A, B, C>.InnerKey key = setItr.next();
			this.parentMap.put(key, tableMetadata.get(key.key1, key.key2));
		}
	}

	public Set<MultiKeyMap<A, B, C>.InnerKey> keySet() {
		return this.parentMap.keySet();
	}
	
	public Map<B,C> keySetFilteredByFirstKey(A firstKey) {
		Set<MultiKeyMap<A, B, C>.InnerKey> keySet = this.parentMap.keySet();
		Map op = new HashMap<B, C>();
		Iterator<MultiKeyMap<A, B, C>.InnerKey> setItr = keySet.iterator();
		while (setItr.hasNext()) {
			MultiKeyMap<A, B, C>.InnerKey key = setItr.next();
			if(key.key1.equals(firstKey))
			{
				op.put(key.key2, this.parentMap.get(key));
			}
		}
		return op;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Set<MultiKeyMap<A, B, C>.InnerKey> keySet = this.parentMap.keySet();
		Iterator<MultiKeyMap<A, B, C>.InnerKey> setItr = keySet.iterator();
		sb.append("[");
		while (setItr.hasNext()) {
			MultiKeyMap<A, B, C>.InnerKey key = setItr.next();
			sb.append("{" + key.key1.toString() + "," + key.key2.toString() + "," + this.parentMap.get(key).toString()
					+ "}");

		}
		sb.append("]");
		return sb.toString();
	}
}
