package com.rationalworks.data.processor.entity;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "virtual-field")
public class VirtualField extends Field{
	private String fieldExpression;

	@XmlElement(name = "field-expression")
	public String getFieldExpression() {
		return fieldExpression;
	}

	public void setFieldExpression(String fieldExpression) {
		this.fieldExpression = fieldExpression;
	}
}
