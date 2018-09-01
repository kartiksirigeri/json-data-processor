package com.rationalworks.data.processor.collection;

import java.util.Set;

public interface IMultiKeyMap <A,B,C> {
	public void put(A key1, B key2, C value);
	public C get(A key1, B key2);
	public Set<MultiKeyMap<A, B, C>.InnerKey> keySet();
	public void putAll(MultiKeyMap<A, B, C> tableMetadata);
	public String toString();
}
