package com.rationalworks.data.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class DataProcessEngineSession {
	private UUID uid;
	private Connection connection;
	
	private Logger logger = Logger.getLogger(DataProcessorEngine.class.getName());

	public DataProcessEngineSession() {
		this.uid = UUID.randomUUID();
	}

	boolean loadJsonData(JSONObject jsonData, String storeName) {
		Object object;
		try {
			Map<String, String> columnsKeys = DataProcessorEngine.getTableMetadata()
					.keySetFilteredByFirstKey(storeName);
			
			String paramString = "";
			StringBuilder insertQueryBuilder = new StringBuilder();
			insertQueryBuilder.append("INSERT INTO");
			insertQueryBuilder.append(" ");
			insertQueryBuilder.append(storeName);
			insertQueryBuilder.append(" ");

			Set<String> columnSet = columnsKeys.keySet();
			Iterator<String> colSetItr = columnSet.iterator();
			insertQueryBuilder.append("(");
			paramString = paramString + "(";
			while (colSetItr.hasNext()) {
				insertQueryBuilder.append(colSetItr.next());
				paramString = paramString + "?";
				if (colSetItr.hasNext()) {
					insertQueryBuilder.append(", ");
					paramString = paramString + ",";
				}
			}
			paramString =  paramString + ",?";
			paramString = paramString + ")";
			insertQueryBuilder.append(",");
			insertQueryBuilder.append(DataProcessorEngine.SESSION_COLUMN);
			insertQueryBuilder.append(")");
			insertQueryBuilder.append(" ");
			insertQueryBuilder.append("values");
			insertQueryBuilder.append(" ");
			insertQueryBuilder.append(paramString);

			String InsertQuery = insertQueryBuilder.toString();
			logger.info("Insert query ["+InsertQuery +"]");
			JSONArray dataSetJson = (JSONArray) jsonData.get("data");

			Iterator<JSONObject> jsonitr = dataSetJson.iterator();
			PreparedStatement insertPreparedStatement = null;
			while (jsonitr.hasNext()) {
				JSONObject obj = jsonitr.next();
				insertPreparedStatement = connection.prepareStatement(InsertQuery);
				Iterator<String> colNameItr = columnSet.iterator();
				int colIndex = 1;
				while (colNameItr.hasNext()) {
					String columnName = colNameItr.next();
					String dataType = columnsKeys.get(columnName);
					if ("VARCHAR".equalsIgnoreCase(dataType)) {
						insertPreparedStatement.setString(colIndex, (String) obj.get(columnName));
					} else {
						Object valueObj = obj.get(columnName);
						if (valueObj instanceof Integer || valueObj instanceof Long) {
							long intToUse = ((Number) valueObj).longValue();
							insertPreparedStatement.setLong(colIndex, intToUse);
						} else if (valueObj instanceof Boolean) {
							Boolean boolToUse = ((Boolean) valueObj).booleanValue();
							insertPreparedStatement.setString(colIndex, boolToUse.toString());

						} else if (valueObj instanceof Float || valueObj instanceof Double) {
							double floatToUse = ((Number) valueObj).doubleValue();
							insertPreparedStatement.setDouble(colIndex, floatToUse);
						} else if (null == valueObj) {
							insertPreparedStatement.setNull(colIndex, Types.NULL);

						}

					}

					colIndex++;
				}
				insertPreparedStatement.setString(colIndex, (String) this.getUid().toString());
				insertPreparedStatement.executeUpdate();
				insertPreparedStatement.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;

	}

	public JSONObject fetchData(String storeName) {
		Map<String, String> columnsKeys = DataProcessorEngine.getTableMetadata()
				.keySetFilteredByFirstKey(storeName);
		
		StringBuilder selectQueryBuilder = new StringBuilder();
		
		selectQueryBuilder.append("select");
		selectQueryBuilder.append(" ");
		Set<String> columnSet = columnsKeys.keySet();
		Iterator<String> colSetItr = columnSet.iterator();
		while (colSetItr.hasNext()) {
			selectQueryBuilder.append(colSetItr.next());
			if(colSetItr.hasNext())
			{
			selectQueryBuilder.append(",");
			}
			selectQueryBuilder.append(" ");
		}
		selectQueryBuilder.append("from");
		selectQueryBuilder.append(" ");
		selectQueryBuilder.append(storeName);
		selectQueryBuilder.append(" ");
		selectQueryBuilder.append("where");
		selectQueryBuilder.append(" ");
		selectQueryBuilder.append(DataProcessorEngine.SESSION_COLUMN);
		selectQueryBuilder.append("=");
		selectQueryBuilder.append("'"+this.uid+"'");
		
		String SelectQuery = selectQueryBuilder.toString();
		logger.info("Select query ["+SelectQuery +"]");
		PreparedStatement selectPreparedStatement = null;
		 JSONObject json = new JSONObject();
		 JSONArray jarr = new JSONArray();
		 json.put("data", jarr);
		try {
			selectPreparedStatement = connection.prepareStatement(SelectQuery);
			ResultSet rs = selectPreparedStatement.executeQuery();

			ResultSetMetaData metaData = rs.getMetaData();

			while (rs.next()) {
				int numColumns = metaData.getColumnCount();
				JSONObject obj = new JSONObject();
				  for( int i=1; i<numColumns+1; i++) {
				    String column_name = metaData.getColumnName(i);

				    switch( metaData.getColumnType( i ) ) {
				      case java.sql.Types.ARRAY:
				        obj.put(column_name, rs.getArray(column_name));     break;
				      case java.sql.Types.BIGINT:
				        obj.put(column_name, rs.getInt(column_name));       break;
				      case java.sql.Types.BOOLEAN:
				        obj.put(column_name, rs.getBoolean(column_name));   break;
				      case java.sql.Types.BLOB:
				        obj.put(column_name, rs.getBlob(column_name));      break;
				      case java.sql.Types.DOUBLE:
				        obj.put(column_name, rs.getDouble(column_name));    break;
				      case java.sql.Types.FLOAT:
				        obj.put(column_name, rs.getFloat(column_name));     break;
				      case java.sql.Types.INTEGER:
				        obj.put(column_name, rs.getInt(column_name));       break;
				      case java.sql.Types.NVARCHAR:
				        obj.put(column_name, rs.getNString(column_name));   break;
				      case java.sql.Types.VARCHAR:
				        obj.put(column_name, rs.getString(column_name));    break;
				      case java.sql.Types.TINYINT:
				        obj.put(column_name, rs.getInt(column_name));       break;
				      case java.sql.Types.SMALLINT:
				        obj.put(column_name, rs.getInt(column_name));       break;
				      case java.sql.Types.DATE:
				        obj.put(column_name, rs.getDate(column_name));      break;
				      case java.sql.Types.TIMESTAMP:
				        obj.put(column_name, rs.getTimestamp(column_name)); break;
				      default:
				        obj.put(column_name, rs.getObject(column_name));    break;
				    }
				  }
				  jarr.add(obj);
			}

			selectPreparedStatement.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return json;

	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public UUID getUid() {
		return uid;
	}
}
