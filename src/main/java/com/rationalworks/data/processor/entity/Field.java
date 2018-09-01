package com.rationalworks.data.processor.entity;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "virtual-field")
public class Field {
	private String name;

	private String dataType;
	
	private boolean isIndexed;

	private boolean isKey;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	@XmlElement(name = "data-type")
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	@XmlElement(name = "is-indexed")
	public boolean isIndexed() {
		return isIndexed;
	}
	public void setIndexed(boolean isIndexed) {
		this.isIndexed = isIndexed;
	}
	
	@XmlElement(name = "is-key")
	public boolean isKey() {
		return isKey;
	}
	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}
}
