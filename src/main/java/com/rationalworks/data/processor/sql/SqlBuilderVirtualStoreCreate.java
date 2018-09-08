package com.rationalworks.data.processor.sql;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.rationalworks.data.processor.DataProcessorEngine;
import com.rationalworks.data.processor.collection.MultiKeyMap;
import com.rationalworks.data.processor.entity.Join;
import com.rationalworks.data.processor.entity.VirtualField;
import com.rationalworks.data.processor.entity.VirtualStore;

public class SqlBuilderVirtualStoreCreate implements BaseSqlBuilder {

	private VirtualStore store;
	
	private MultiKeyMap<String,String,String> tableMetadata= new MultiKeyMap<String, String,String>();
	
	public SqlBuilderVirtualStoreCreate(VirtualStore store) {
		this.store = store;
	}

	public String generateCreateSQL() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("create view");
		sb.append(" ");
		sb.append(this.store.getName());
		sb.append(" ");
		sb.append("(");
		
		
		StringBuilder outputColumns = new StringBuilder();
		StringBuilder selectedColumns = new StringBuilder();
		
		Set<String> fieldExpressions = new HashSet<String>();
		
		Iterator<VirtualField> fieldIterator = store.getFields().iterator();
		while (fieldIterator.hasNext()) {
			VirtualField field = fieldIterator.next();
			outputColumns.append(field.getName());
			selectedColumns.append(field.getFieldExpression());
			selectedColumns.append(" as ");
			selectedColumns.append(field.getName());
			if(fieldIterator.hasNext())
			{
				outputColumns.append(",");
				selectedColumns.append(",");
			}
			tableMetadata.put(store.getName(),field.getName(), null);
			fieldExpressions.add(field.getName());
		}
		sb.append(outputColumns.toString());
		sb.append(",");
		sb.append(DataProcessorEngine.SESSION_COLUMN);
		sb.append(")");
		sb.append(" ");
		sb.append("as");
		sb.append(" ");
		sb.append("select");
		sb.append(" ");
		sb.append(selectedColumns);
		sb.append(",");
		if(null != this.store.getSourceStore())
		{
			sb.append(DataProcessorEngine.SESSION_COLUMN);
		}else if(null != this.store.getJoins())
		{
			sb.append(this.store.getJoins().get(0).getLeftAlias()+"."+DataProcessorEngine.SESSION_COLUMN);
		}
		
		sb.append(" ");
		sb.append("from");
		sb.append(" ");
		if(null != this.store.getSourceStore())
		{
		sb.append(this.store.getSourceStore());
		}else if(null != this.store.getJoins())
		{
			Iterator<Join> sItr = this.store.getJoins().iterator();
			while(sItr.hasNext())
			{
				Join j = sItr.next();
				JoinSQLBuilder jqb = new JoinSQLBuilder(j);
				sb.append(jqb.generateCreateSQL());
			}
			
		}
		sb.append(" ");
		if(this.store.getGroupBy() != null && this.store.getGroupBy().size()>0)
		{
			sb.append("group by");
			sb.append(" ");
			List<String> groupByList = this.store.getGroupBy();
			Iterator<String> groupByItr = groupByList.iterator();
			while(groupByItr.hasNext())
			{
				String groupByCaluse = groupByItr.next();
				sb.append(groupByCaluse);
				if(groupByItr.hasNext())
				{
					sb.append(",");
				}
			}
			sb.append(",");
			sb.append(" ");
			if(null != this.store.getSourceStore())
			{
				sb.append(DataProcessorEngine.SESSION_COLUMN);
			}else if(null != this.store.getJoins())
			{
				sb.append(this.store.getJoins().get(0).getLeftAlias()+"."+DataProcessorEngine.SESSION_COLUMN);
			}
			
		}else
		{
			sb.append("group by");
			sb.append(" ");
			if(null != this.store.getSourceStore())
			{
				sb.append(DataProcessorEngine.SESSION_COLUMN);
			}else if(null != this.store.getJoins())
			{
				sb.append(this.store.getJoins().get(0).getLeftAlias()+"."+DataProcessorEngine.SESSION_COLUMN);
			}
			
			sb.append(" ");
			/*
			sb.append(",");
			sb.append(" ");
			// add group by fileds
			Iterator<String> fieldExpressionsItr = fieldExpressions.iterator();
			while(fieldExpressionsItr.hasNext())
			{
				sb.append(fieldExpressionsItr.next());
				if(fieldExpressionsItr.hasNext())
				{
					sb.append(" ");
					sb.append(",");
				}
			}
			*/
		}
		sb.append(";");

		return sb.toString();
	}

	public MultiKeyMap<String, String, String> getTableMetadata() {
		return tableMetadata;
	}

	public void setTableMetadata(MultiKeyMap<String, String, String> tableMetadata) {
		this.tableMetadata = tableMetadata;
	}

}
