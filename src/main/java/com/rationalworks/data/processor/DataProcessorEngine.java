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
import java.util.HashMap;
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
import com.rationalworks.data.processor.entity.Field;
import com.rationalworks.data.processor.xml.XMLFileProcessor;

public class DataProcessorEngine {
	private static Server server = null;
	private static JdbcConnectionPool connectionPool = null;
	private static Server webServer = null;
	private static MultiKeyMap<String, String, String> tableMetadata = new MultiKeyMap<String, String, String>();

	static Logger logger = Logger.getLogger(DataProcessorEngine.class.getName());

	public static void initilize() {

		// start the TCP Server
		try {
			webServer = Server.createWebServer("-web", "-webPort", "8082").start();
			// server = Server.createTcpServer(new String[] { "-tcpPort",
			// "8092", "-tcpAllowOthers" }).start();
			setConnectionPool(JdbcConnectionPool.create("jdbc:h2:mem:storage", "sa", "sa"));

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
				tableMetadata.putAll(xfp.getTableMetadata());

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

	public static void loadOutputData(String storeName) {
		String SelectQuery = "select * from " + storeName;
		PreparedStatement selectPreparedStatement = null;
		try {
			Connection connection = connectionPool.getConnection();
			selectPreparedStatement = connection.prepareStatement(SelectQuery);
			ResultSet rs = selectPreparedStatement.executeQuery();

			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();

			while (rs.next()) {
				for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
					Object object = rs.getObject(columnIndex);
					System.out.printf("%s, ", object == null ? "NULL" : object.toString());
				}
				System.out.printf("%n");
			}

			selectPreparedStatement.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void loadJsonData(String inputDataFile, String destinationStore) {
		JSONParser parser = new JSONParser();
		Object object;
		try {
			Map<String, String> columnsKeys = tableMetadata.keySetFilteredByFirstKey(destinationStore);
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

			/*
			 * Iterator<Person> personItr = inputData.iterator(); while
			 * (personItr.hasNext()) { Person item = personItr.next();
			 * Connection connection = connectionPool.getConnection();
			 * insertPreparedStatement =
			 * connection.prepareStatement(InsertQuery);
			 * insertPreparedStatement.setInt(1, item.getId());
			 * insertPreparedStatement.setString(2, item.getName());
			 * insertPreparedStatement.setInt(3, item.getAge());
			 * insertPreparedStatement.executeUpdate();
			 * insertPreparedStatement.close(); connection.close(); }
			 */
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

}
