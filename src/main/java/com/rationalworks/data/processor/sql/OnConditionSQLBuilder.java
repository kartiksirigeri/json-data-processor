package com.rationalworks.data.processor.sql;

import java.util.Iterator;

import com.rationalworks.data.processor.entity.OnCondition;

public class OnConditionSQLBuilder implements BaseSqlBuilder {

	private OnCondition condition;

	public OnConditionSQLBuilder(OnCondition condition) {
		this.condition = condition;
	}

	public String generateCreateSQL() {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		if (null != condition.getConditionExpression()) {
			Iterator<String> expItr = condition.getConditionExpression().iterator();
			while(expItr.hasNext())
			{
				sb.append(" ");
				sb.append(expItr.next());
				if (expItr.hasNext()) {
					sb.append(" ");
					sb.append(condition.getType());
				}	
			}
			
		}
		
		if (null != condition.getConditions()) {
			sb.append(" ");
			sb.append(condition.getType());
			sb.append(" ");
			Iterator<OnCondition> condItr = condition.getConditions().iterator();
			while(condItr.hasNext())
			{
				sb.append(" ");
				OnConditionSQLBuilder sqb  = new OnConditionSQLBuilder(condItr.next());
				sb.append(sqb.generateCreateSQL());
				if (condItr.hasNext()) {
					sb.append(" ");
					sb.append(condition.getType());
				}
			}
		}
		
		sb.append(")");
		return sb.toString();
	}

}
