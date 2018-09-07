package com.rationalworks.data.processor.entity;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "virtual-store")
public class VirtualStore {
	private String name;
	
	private List<VirtualField> fields;
	
	private List<String> groupBy;
	
	private String sourceStore;
	
	private List<Join> joins;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	@XmlElement(name = "source-store")
	public String getSourceStore() {
		return sourceStore;
	}
	public void setSourceStore(String sourceStore) {
		this.sourceStore = sourceStore;
	}
	@XmlElement(name = "virtual-fields")
	public List<VirtualField> getFields() {
		return fields;
	}
	public void setFields(List<VirtualField> fields) {
		this.fields = fields;
	}
	
	@XmlElement(name = "goup-by")
	public List<String> getGroupBy() {
		return groupBy;
	}
	public void setGroupBy(List<String> groupBy) {
		this.groupBy = groupBy;
	}

	 @XmlElementWrapper(name = "joins")
	 @XmlElement(name = "join")
	public List<Join> getJoins() {
		return joins;
	}
	public void setJoins(List<Join> joins) {
		this.joins = joins;
	}
}
