package com.rationalworks.data.processor.entity;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "stores")
public class Stores {
	private List<Store> store;
	private List<VirtualStore> virtualStore;

	public List<Store> getStore() {
		return store;
	}

	public void setStore(List<Store> store) {
		this.store = store;
	}

	@XmlElement(name = "virtual-stores")
	public List<VirtualStore> getVirtualStore() {
		return virtualStore;
	}

	public void setVirtualStore(List<VirtualStore> virtualStore) {
		this.virtualStore = virtualStore;
	}


}
