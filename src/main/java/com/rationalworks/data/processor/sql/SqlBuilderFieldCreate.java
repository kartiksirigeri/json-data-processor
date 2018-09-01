package com.rationalworks.data.processor.sql;

import com.rationalworks.data.processor.entity.Field;

public class SqlBuilderFieldCreate implements BaseSqlBuilder{

	private Field field;
	
	public SqlBuilderFieldCreate(Field field)
	{
		this.field = field;
	}
	
	public String generateCreateSQL() {
		StringBuilder sb = new StringBuilder();
		sb.append(field.getName());
		sb.append(" ");
		sb.append(field.getDataType());
		sb.append(" ");
		return sb.toString();
	}

}
