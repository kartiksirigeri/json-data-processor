package com.rationalworks.data.processor.entity;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "store")
public class Store {
	private String name;
	private String terminationType;
	private List<Field> filelds;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	@XmlElement(name = "termination-type")
	public String getTerminationType() {
		return terminationType;
	}
	public void setTerminationType(String terminationType) {
		this.terminationType = terminationType;
	}
	public List<Field> getFilelds() {
		return filelds;
	}
	public void setFilelds(List<Field> filelds) {
		this.filelds = filelds;
	}
}
