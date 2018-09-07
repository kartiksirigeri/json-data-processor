/**
 * 
 */
package com.rationalworks.data.processor.sql;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXB;

import com.rationalworks.data.processor.entity.Join;
import com.rationalworks.data.processor.entity.OnCondition;
import com.rationalworks.data.processor.entity.VirtualField;
import com.rationalworks.data.processor.entity.VirtualStore;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author Praveen
 *
 */
public class SqlBuilderVirtualStoreCreatetTest extends TestCase {

	VirtualStore vstore;
	Join j1;
	Join j2;
	OnCondition c3;
	/**
	 * @param name
	 */
	public SqlBuilderVirtualStoreCreatetTest(String name) {
		super(name);
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		vstore = new VirtualStore();
		vstore.setName("employmentperyearwithmean");
		
		VirtualField f1 = new VirtualField();
		f1.setName("series");
		f1.setFieldExpression("lr.series");
		
		VirtualField f2 = new VirtualField();
		f2.setFieldExpression("rt.avg_employment_count");
		f2.setName("avg_employment_count");
		
		List fields  =new ArrayList<VirtualField>();
		fields.add(f1);
		fields.add(f2);
		
		vstore.setFields(fields);
		
		List<String> exprList = new ArrayList<String>();
		exprList.add("lt.series=rt.series");
		exprList.add("lt.series=rt.series");
		
		OnCondition c1 = new OnCondition();
		c1.setType("and");
		c1.setConditionExpression(exprList);
		
		OnCondition c2 = new OnCondition();
		c2.setType("or");
		c2.setConditionExpression(exprList);
		
		c3 = new OnCondition();
		c3.setType("and");
		c3.setConditionExpression(exprList);
		List<OnCondition> conditionList = new ArrayList<OnCondition>();
		conditionList.add(c1);
		conditionList.add(c2);
		c3.setConditions(conditionList);
		
		j1 = new Join();
		j1.setLeftStore("employmentperyear");
		j1.setRightStore("avgemploymentperyear");
		j1.setLeftAlias("l1");
		j1.setRightAlias("l2");
		j1.setJoinType("inner");
		
		List<OnCondition> conditions = new ArrayList<OnCondition>();
		conditions.add(c1);
		j1.setConditions(conditions);
		
		
		j2 = new Join();
		j2.setLeftStore("employmentperyear");
		j2.setRightStore("avgemploymentperyear");
		j2.setLeftAlias("l3");
		j2.setRightAlias("l4");
		j2.setJoinType("left outer");
		
		List<OnCondition> conditions2 = new ArrayList<OnCondition>();
		conditions2.add(c3);
		j2.setConditions(conditions2);
		
		
		List joins  =new ArrayList<Join>();
		//joins.add(j1);
		joins.add(j2);
		
		vstore.setJoins(joins);
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for {@link com.rationalworks.data.processor.sql.SqlBuilderVirtualStoreCreate#generateCreateSQL()}.
	 */
	public void testGenerateCreateSQL() {
		
		StringWriter sw = new StringWriter();
		JAXB.marshal(vstore, sw);
		String xmlString = sw.toString();
		System.out.println(xmlString);
		
		OnConditionSQLBuilder sqb = new OnConditionSQLBuilder(c3);
		//System.out.println(sqb.generateCreateSQL());
		
		JoinSQLBuilder jsb = new JoinSQLBuilder(j2);
		//System.out.println(jsb.generateCreateSQL());
		  assertTrue( false );
		fail("Not yet implemented");

	}
	
	public void testVstoreSerialization() {
		
		StringWriter sw = new StringWriter();
		JAXB.marshal(vstore, sw);
		String xmlString = sw.toString();
		System.out.println(xmlString);

		VirtualStore restored = JAXB.unmarshal(xmlString, VirtualStore.class);
		
		StringWriter reserialized = new StringWriter();
		JAXB.marshal(restored, reserialized);
		String xmlString2 = reserialized.toString();
		System.out.println(xmlString2);
		Assert.assertEquals(xmlString, xmlString2);

	}

}
