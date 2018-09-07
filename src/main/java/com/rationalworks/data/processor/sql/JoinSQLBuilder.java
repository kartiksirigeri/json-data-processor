package com.rationalworks.data.processor.sql;

import java.util.Iterator;

import com.rationalworks.data.processor.DataProcessorEngine;
import com.rationalworks.data.processor.entity.Join;
import com.rationalworks.data.processor.entity.OnCondition;

public class JoinSQLBuilder implements BaseSqlBuilder {

	private Join join;

	public JoinSQLBuilder(Join join) {
		this.join = join;
	}

	public String generateCreateSQL() {
		StringBuilder sb = new StringBuilder();
		sb.append(" ");
		sb.append(join.getLeftStore());
		sb.append(" ");
		sb.append(join.getLeftAlias());
		sb.append(" ");
		sb.append(join.getJoinType());
		sb.append(" ");
		sb.append(join.getRightStore());
		sb.append(" ");
		sb.append(join.getRightAlias());
		if (null != join.getConditions()) {
			Iterator<OnCondition> conItr = join.getConditions().iterator();
			sb.append(" ");
			sb.append("on");
			sb.append(" ");
			while(conItr.hasNext())
			{
				OnCondition condition = conItr.next();
				OnConditionSQLBuilder sqb  = new OnConditionSQLBuilder(condition);
				sb.append(sqb.generateCreateSQL());
			}
		}
		sb.append(" ");
		sb.append("and");
		sb.append(" ");
		String additionOnClause = join.getLeftAlias()+"."+DataProcessorEngine.SESSION_COLUMN +"=" +join.getRightAlias()+"."+DataProcessorEngine.SESSION_COLUMN;
		sb.append(additionOnClause);
		sb.append(" ");
		return sb.toString();
	}

}
