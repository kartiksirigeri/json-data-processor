package com.rationalworks.data.processor.entity;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

public class Join {
	private String leftStore;
	private String rightStore;
	private String leftAlias;
	private String rightAlias;
	private String joinType; /* inner, outer, cross, left outer, right outer */
	private List<OnCondition> conditions;

	@XmlElement(name = "left-store")
	public String getLeftStore() {
		return leftStore;
	}

	@XmlElement(name = "right-store")
	public String getRightStore() {
		return rightStore;
	}

	@XmlElement(name = "left-store-alias")
	public String getLeftAlias() {
		return leftAlias;
	}

	@XmlElement(name = "right-store-alias")
	public String getRightAlias() {
		return rightAlias;
	}

	@XmlElement(name = "join-type")
	public String getJoinType() {
		return joinType;
	}

	@XmlElementWrapper(name = "on-conditions")
	@XmlElement(name = "on-condition")
	public List<OnCondition> getConditions() {
		return conditions;
	}

	public void setLeftStore(String leftStore) {
		this.leftStore = leftStore;
	}

	public void setRightStore(String rightStore) {
		this.rightStore = rightStore;
	}

	public void setLeftAlias(String leftAlias) {
		this.leftAlias = leftAlias;
	}

	public void setRightAlias(String rightAlias) {
		this.rightAlias = rightAlias;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public void setConditions(List<OnCondition> conditions) {
		this.conditions = conditions;
	}
}
