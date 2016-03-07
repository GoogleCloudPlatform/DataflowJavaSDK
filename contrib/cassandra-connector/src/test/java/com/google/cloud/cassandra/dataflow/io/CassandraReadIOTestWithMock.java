package com.google.cloud.cassandra.dataflow.io;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import com.datastax.driver.mapping.annotations.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class CassandraReadIOTestWithMock {
	private static Class entityName;
	private static String query;
	
	private static CassandraReadIO.Read.CassandraReadOperation mockCassandraRead;
	private static 	List lstEmp;
	private static Pipeline p;
	
	/**
	 * Setup
	 *
	 */
	@Before
	public void setUp() {
		p=Mockito.mock(Pipeline.class);
		mockCassandraRead = Mockito.mock(CassandraReadIO.Read.CassandraReadOperation.class);
		lstEmp = Mockito.mock(ArrayList.class);
		
	}
	/**
	 * Performs a CassandraReadIO test to check whether method returns PCollection object or not
	 *
	 */
	@Test
	public void testToGetPCollection() {
		
		CassandraReadIOTestWithMock.EmployeeDetails employeeDetails= Mockito.mock(CassandraReadIOTestWithMock.EmployeeDetails.class);
		
		lstEmp.add(employeeDetails);
		PCollection mockPCollection =  Mockito.mock(PCollection.class);
		
		Mockito.when(mockCassandraRead.apply()).thenReturn(lstEmp);
		
		Mockito.doReturn(mockPCollection).when(p).apply(Create.of(lstEmp));
		p.run();
		Assert.assertNotNull(mockPCollection);
	}

	/**
	 * Class that will be passed as entity
	 *
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
