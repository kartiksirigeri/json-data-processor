package com.rationalworks.data.processor;

import java.util.List;

public class InputData {
	private String store;
	private List<Person> persons;
	
	public String getStore() {
		return store;
	}
	public void setStore(String store) {
		this.store = store;
	}
	public List<Person> getPersons() {
		return persons;
	}
	public void setPersons(List<Person> persons) {
		this.persons = persons;
	}
}
