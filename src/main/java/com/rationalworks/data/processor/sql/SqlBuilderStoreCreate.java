package com.rationalworks.data.processor.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.rationalworks.data.processor.DataProcessorEngine;
import com.rationalworks.data.processor.collection.MultiKeyMap;
import com.rationalworks.data.processor.entity.Field;
import com.rationalworks.data.processor.entity.Store;

public class SqlBuilderStoreCreate implements BaseSqlBuilder {

	private Store store;
	
	// TableName,ColumnName,DataType
	private MultiKeyMap<String,String,String> tableMetadata= new MultiKeyMap<String, String,String>();

	public MultiKeyMap<String, String, String> getTableMetadata() {
		return tableMetadata;
	}

	public SqlBuilderStoreCreate(Store store) {
		this.store = store;
	}

	public String generateCreateSQL() {
		List<String> indexList = new ArrayList<String>();
		
		StringBuilder sb = new StringBuilder();
		sb.append("create table");
		sb.append(" ");
		sb.append(this.store.getName());
		sb.append(" ");
		sb.append("(");
		Iterator<Field> fieldIterator = store.getFilelds().iterator();
		while (fieldIterator.hasNext()) {
			Field field = fieldIterator.next();
			SqlBuilderFieldCreate sfc = new SqlBuilderFieldCreate(field);
			sb.append(sfc.generateCreateSQL());
			if (fieldIterator.hasNext()) {
				sb.append(",");
			}
			if(field.isIndexed())
			{
				indexList.add("create INDEX "+ this.store.getName()+"_"+field.getName()+" on "+this.store.getName()+"("+field.getName()+");");
			}
			tableMetadata.put(store.getName(),field.getName(), field.getDataType());
		}
		sb.append(","+DataProcessorEngine.SESSION_COLUMN+" varchar");
		sb.append(");");
		 Iterator<String> indexListItr = indexList.iterator();
		 while(indexListItr.hasNext())
		 {
			 sb.append(indexListItr.next());
		 }
		
		return sb.toString();
	}

}
