package com.rationalworks.data.processor.sql;

import java.util.Iterator;
import java.util.List;

import com.rationalworks.data.processor.DataProcessorEngine;
import com.rationalworks.data.processor.collection.MultiKeyMap;
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
		Iterator<VirtualField> fieldIterator = store.getFields().iterator();
		
		StringBuilder outputColumns = new StringBuilder();
		StringBuilder selectedColumns = new StringBuilder();
		
		while (fieldIterator.hasNext()) {
			VirtualField field = fieldIterator.next();
			outputColumns.append(field.getName());
			selectedColumns.append(field.getFieldExpression());
			if(fieldIterator.hasNext())
			{
				outputColumns.append(",");
				selectedColumns.append(",");
			}
			tableMetadata.put(store.getName(),field.getName(), null);
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
		sb.append(DataProcessorEngine.SESSION_COLUMN);
		sb.append(" ");
		sb.append("from");
		sb.append(" ");
		sb.append(this.store.getSourceStore());
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
			sb.append(DataProcessorEngine.SESSION_COLUMN);
			
		}else
		{
			sb.append("group by");
			sb.append(" ");
			sb.append(DataProcessorEngine.SESSION_COLUMN);
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
