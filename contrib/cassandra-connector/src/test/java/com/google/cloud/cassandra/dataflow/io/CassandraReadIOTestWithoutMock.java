package com.google.cloud.cassandra.dataflow.io;
import java.io.Serializable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import cassandra.utils.Utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
/**
* Class contains JUnit test case that can be tested in cloud. 
*/
public class CassandraReadIOTestWithoutMock {
	private static String[] hosts;
	private static int port;
	private static String keyspace;
	private static Class entityName;
	private static String query;
	private static String conditionalBasedQuery;
	
	private static Cluster cluster;
	private static Session session;
	private static MappingManager manager;
	
	static PipelineOptions options;
	static Pipeline p;
	
	static CassandraReadIO.Read.Bound bound;
	static CassandraReadIO.Read.CassandraReadOperation<String> cassandraRead;

	/**
	 * Initial setup for cassandra connection
	 * hosts : cassandra server hosts
	 * keyspace : schema name
	 * port : port of the cassandra server
	 * entityName : is the POJO class
	 * query : simple query
	 * conditionalBasedQuery : conditional based query
	 */
	@BeforeClass
	public static void oneTimeSetUp() {
		hosts = new String[] { "localhost" };
		keyspace = "demo1";
		port = 9042;
		entityName = CassandraReadIOTestWithoutMock.EmployeeDetails.class;
		query = QueryBuilder.select().all()
				.from(Utils.KEYSPACE, "emp_info").toString();
		
		conditionalBasedQuery = QueryBuilder.select().from(Utils.KEYSPACE, "emp_info").where(QueryBuilder.eq("emp_id", 1)).toString();
	}

	/**
	 * Creating a pipeline
	 */
	@Before
	public void setUp() {
		options = PipelineOptionsFactory.create();
		p = Pipeline.create(options);
	}
	
	/**
	 * Test for checking PCollection is not null
	 */
	@Test
	public void testToGetPCollection() {
		bound = new CassandraReadIO.Read.Bound(hosts, keyspace, port, entityName, query);
		cassandraRead = new  CassandraReadIO.Read.CassandraReadOperation<>(bound);
		PCollection pcollection = p.apply(Create.of(cassandraRead.apply()));
		p.run();
		Assert.assertNotNull(pcollection);
	}

	/**
	 * Test for conditional based query
	 */
	@Test
	public void testForConditionalBased() {
		bound = new CassandraReadIO.Read.Bound(hosts, keyspace, port, entityName, conditionalBasedQuery);
		cassandraRead = new  CassandraReadIO.Read.CassandraReadOperation<>(bound);
		Assert.assertSame(1, cassandraRead.apply().size());
		PCollection pcollection = p.apply(Create.of(cassandraRead.apply()));
		p.run();
		Assert.assertNotNull(pcollection);
	}

	/**
	 * static inner class contains employee details
	 */
	@Table(name = "emp_info") 
	public static class EmployeeDetails implements Serializable{
	/**
		 * 
		 */
	private static final long serialVersionUID = 1L;
	private int emp_id;
	private String emp_first;
	private String emp_last;
	private String emp_address;
	private String emp_dept;
	public EmployeeDetails(){
		
	}
	public int getEmp_id() {
		return emp_id;
	}
	public void setEmp_id(int emp_id) {
		this.emp_id = emp_id;
	}
	public String getEmp_first() {
		return emp_first;
	}
	public void setEmp_first(String emp_first) {
		this.emp_first = emp_first;
	}
	public String getEmp_last() {
		return emp_last;
	}
	public void setEmp_last(String emp_last) {
		this.emp_last = emp_last;
	}
	public String getEmp_address() {
		return emp_address;
	}
	public void setEmp_address(String emp_address) {
		this.emp_address = emp_address;
	}
	public String getEmp_dept() {
		return emp_dept;
	}
	public void setEmp_dept(String emp_dept) {
		this.emp_dept = emp_dept;
	}

	}


}
