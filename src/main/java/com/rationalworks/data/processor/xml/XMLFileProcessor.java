package com.rationalworks.data.processor.xml;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.rationalworks.data.processor.collection.MultiKeyMap;
import com.rationalworks.data.processor.entity.Store;
import com.rationalworks.data.processor.entity.Stores;
import com.rationalworks.data.processor.entity.VirtualStore;
import com.rationalworks.data.processor.sql.SqlBuilderStoreCreate;
import com.rationalworks.data.processor.sql.SqlBuilderVirtualStoreCreate;

public class XMLFileProcessor {
	private File file;
	private MultiKeyMap<String, String, String> tableMetadata = new MultiKeyMap<String, String, String>();

	public MultiKeyMap<String, String, String> getTableMetadata() {
		return tableMetadata;
	}

	static Logger logger = Logger.getLogger(XMLFileProcessor.class.getName());

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public List<String> getDDLs() {
		Stores stores = null;
		List<String> storeList = new ArrayList<String>();
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContext.newInstance(Stores.class);
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			stores = (Stores) unmarshaller.unmarshal(file);
		} catch (JAXBException e) {
			e.printStackTrace();
		}

		Iterator<Store> storeIterator = stores.getStore().iterator();
		while (storeIterator.hasNext()) {
			SqlBuilderStoreCreate ssc = new SqlBuilderStoreCreate(storeIterator.next());
			String ddl = ssc.generateCreateSQL();
			logger.info("DDL[" + ddl + "]");
			storeList.add(ddl);
			tableMetadata.putAll(ssc.getTableMetadata());
		}

		if (null != stores.getVirtualStore()) {
			Iterator<VirtualStore> virtualStoreIterator = stores.getVirtualStore().iterator();
			while (virtualStoreIterator.hasNext()) {
				SqlBuilderVirtualStoreCreate ssc = new SqlBuilderVirtualStoreCreate(virtualStoreIterator.next());
				String ddl = ssc.generateCreateSQL();
				logger.info("DDL[" + ddl + "]");
				storeList.add(ddl);
				tableMetadata.putAll(ssc.getTableMetadata());
			}
		}
		return storeList;

	}
}
