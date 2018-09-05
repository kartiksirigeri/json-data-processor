package com.rationalworks.data.processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.Server;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.rationalworks.data.processor.collection.MultiKeyMap;
import com.rationalworks.data.processor.xml.XMLFileProcessor;

public class DataProcessorEngine {
	private static Server server = null;
	private static JdbcConnectionPool connectionPool = null;
	private static MultiKeyMap<String, String, String> tableMetadata = new MultiKeyMap<String, String, String>();
	public static String SESSION_COLUMN = "dps_sessionuid";

	static Logger logger = Logger.getLogger(DataProcessorEngine.class.getName());

	public static void initilize() {

		//
		// server = Server.createTcpServer(new String[] { "-tcpPort","8092", "-tcpAllowOthers" }).start();
		if(null == DataProcessorEngine.connectionPool)
		{
			setConnectionPool(JdbcConnectionPool.create("jdbc:h2:mem:storage", "sa", "sa"));
		}

	}
	
	public static void initilize(int i) {
		try {
			server = Server.createWebServer("-web", "-webPort", "8082").start();
			initilize();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void shutdown() {
		// stop the TCP Server
		server.stop();
	}

	public static Server getServer() {
		return server;
	}

	public static void setServer(Server server) {
		DataProcessorEngine.server = server;
	}

	public static void loadXmls(String folderWithStoreDefinitions) {
		File definitionFoler = new File(folderWithStoreDefinitions);
		logger.info("Looking for schema files at " + definitionFoler.getAbsolutePath());
		if (definitionFoler.isDirectory()) {
			logger.info("Loading schema from " + folderWithStoreDefinitions);
			File[] files = definitionFoler.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return name.endsWith(".xml");
				}
			});

			for (int i = 0; i < files.length; i++) {
				logger.info("Processing file " + files[i].getAbsolutePath());
				XMLFileProcessor xfp = new XMLFileProcessor();
				xfp.setFile(files[i]);
				List<String> ddls = xfp.getDDLs();
				getTableMetadata().putAll(xfp.getTableMetadata());

				Iterator<String> ddlItr = ddls.iterator();
				while (ddlItr.hasNext()) {
					String ddlSQL = ddlItr.next();
					PreparedStatement createPreparedStatement;
					try {
						Connection connection = connectionPool.getConnection();
						createPreparedStatement = connection.prepareStatement(ddlSQL);
						createPreparedStatement.executeUpdate();
						createPreparedStatement.close();
						connection.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
		} else {
			logger.info("File path " + folderWithStoreDefinitions + " is not a directory");
		}

	}

	public static JdbcConnectionPool getConnectionPool() {
		return connectionPool;
	}

	public static void setConnectionPool(JdbcConnectionPool connectionPool) {
		DataProcessorEngine.connectionPool = connectionPool;
	}

	public static void loadInputData(List<Person> inputData) {
		String InsertQuery = "INSERT INTO datasource_table1" + "(id, name, age) values" + "(?,?,?)";
		PreparedStatement insertPreparedStatement = null;

		Iterator<Person> personItr = inputData.iterator();
		while (personItr.hasNext()) {
			Person item = personItr.next();
			try {
				Connection connection = connectionPool.getConnection();
				insertPreparedStatement = connection.prepareStatement(InsertQuery);
				insertPreparedStatement.setInt(1, item.getId());
				insertPreparedStatement.setString(2, item.getName());
				insertPreparedStatement.setInt(3, item.getAge());
				insertPreparedStatement.executeUpdate();
				insertPreparedStatement.close();
				connection.close();

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public static JSONObject loadOutputData(String storeName) {
		String SelectQuery = "select * from " + storeName;
		PreparedStatement selectPreparedStatement = null;
		 JSONObject json = new JSONObject();
		 JSONArray jarr = new JSONArray();
		 json.put("data", jarr);
		try {
			Connection connection = connectionPool.getConnection();
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

	public static void loadJsonData(String inputDataFile, String destinationStore) {
		JSONParser parser = new JSONParser();
		Object object;
		try {
			Map<String, String> columnsKeys = getTableMetadata().keySetFilteredByFirstKey(destinationStore);
			String paramString = "";
			StringBuilder insertQueryBuilder = new StringBuilder();
			insertQueryBuilder.append("INSERT INTO");
			insertQueryBuilder.append(" ");
			insertQueryBuilder.append(destinationStore);
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
			paramString = paramString + ")";
			insertQueryBuilder.append(")");
			insertQueryBuilder.append(" ");
			insertQueryBuilder.append("values");
			insertQueryBuilder.append(" ");
			insertQueryBuilder.append(paramString);

			String InsertQuery = insertQueryBuilder.toString();
			object = parser.parse(new FileReader(new File(inputDataFile)));
			// convert Object to JSONObject
			JSONObject inputJson = (JSONObject) object;
			JSONArray dataSetJson = (JSONArray) inputJson.get("data");
			// System.out.println(dataSetJson.toJSONString());
			// System.out.println(tableMetadata);
			// System.out.println(insertQueryBuilder.toString());

			Iterator<JSONObject> jsonitr = dataSetJson.iterator();
			PreparedStatement insertPreparedStatement = null;
			while (jsonitr.hasNext()) {
				JSONObject obj = jsonitr.next();
				Connection connection = connectionPool.getConnection();
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

				insertPreparedStatement.executeUpdate();
				insertPreparedStatement.close();
				connection.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static DataProcessEngineSession getSession() {
		DataProcessEngineSession dpsession  = new DataProcessEngineSession();
		try {
			dpsession.setConnection( connectionPool.getConnection());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dpsession;
	}

	public static MultiKeyMap<String, String, String> getTableMetadata() {
		return tableMetadata;
	}

	public static void setTableMetadata(MultiKeyMap<String, String, String> tableMetadata) {
		DataProcessorEngine.tableMetadata = tableMetadata;
	}

	

}
