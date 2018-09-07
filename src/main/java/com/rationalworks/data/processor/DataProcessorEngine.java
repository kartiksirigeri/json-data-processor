package com.rationalworks.data.processor;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.Server;

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
