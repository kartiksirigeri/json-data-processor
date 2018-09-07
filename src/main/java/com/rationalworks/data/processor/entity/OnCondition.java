package com.rationalworks.data.processor.entity;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "on-condition")
public class OnCondition {
	private String type;
	private List<String> conditionExpression;
	private List<OnCondition> conditions;
	
	@XmlElement(name = "on-condition-type")
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	@XmlElement(name = "condition-expression")
	public List<String> getConditionExpression() {
		return conditionExpression;
	}
	public void setConditionExpression(List<String> conditionExpression) {
		this.conditionExpression = conditionExpression;
	}
	
	@XmlElementWrapper(name = "on-conditions")
	 @XmlElement(name = "on-condition")
	public List<OnCondition> getConditions() {
		return conditions;
	}
	public void setConditions(List<OnCondition> conditions) {
		this.conditions = conditions;
	}
}
