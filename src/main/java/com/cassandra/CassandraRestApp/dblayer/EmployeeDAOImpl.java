package com.cassandra.CassandraRestApp.dblayer;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import com.cassandra.CassandraRestApp.model.Employee;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

@Repository
public class EmployeeDAOImpl implements EmployeeDAO {

	@Autowired
	private CassandraOperations cassandraTemplate;

	@Override
	public Employee createEmployee(Employee employee) {
		String insertPreparedStmt = "INSERT INTO employee(id, name, age, salary) VALUES(?,?,?,?)";
		List<Object> singleEmployeeArgsList = new ArrayList<>();
		List<List<?>> employeeList = new ArrayList<>();
		singleEmployeeArgsList.add(employee.getId());
		singleEmployeeArgsList.add(employee.getName());
		singleEmployeeArgsList.add(employee.getAge());
		singleEmployeeArgsList.add(employee.getSalary());
		employeeList.add(singleEmployeeArgsList);
		cassandraTemplate.ingest(insertPreparedStmt, employeeList);
		return employee;
	}

	@Override
	public Employee getEmployee(int id) {
		Select select = QueryBuilder.select("id", "name", "age", "salary").from("employee");
		select.where(QueryBuilder.eq("id", id));
		/*
		 * Employee employee = cassandraTemplate.queryForObject(select, new
		 * RowMapper<Employee>(){
		 * 
		 * @Override public Employee mapRow(Row row, int rowNum) throws
		 * DriverException { Employee employee = new Employee(row.getLong("id"),
		 * row.getString("name"), row.getInt("age"), row.getFloat("salary"));
		 * return employee; }
		 * 
		 * });
		 */
		Employee employee = cassandraTemplate.selectOne(select, Employee.class);
		return employee;
	}

	@Override
	public Employee updateEmployee(Employee employee) {
		return cassandraTemplate.update(employee);
//		return myCassandraTemplate.update(employee, Employee.class);
	}

	@Override
	public void deleteEmployee(int id) {
		Select select = QueryBuilder.select("id", "name", "age", "salary").from("employee");
		select.where(QueryBuilder.eq("id", id));
		
		cassandraTemplate.deleteById(Employee.class, id);
		
	}

	@Override
	public List<Employee> getAllEmployees() {
		Select select = QueryBuilder.select("id", "name", "age", "salary").from("employee");
		/*
		 * Employee employee = cassandraTemplate.queryForObject(select, new
		 * RowMapper<Employee>(){
		 * 
		 * @Override public Employee mapRow(Row row, int rowNum) throws
		 * DriverException { Employee employee = new Employee(row.getLong("id"),
		 * row.getString("name"), row.getInt("age"), row.getFloat("salary"));
		 * return employee; }
		 * 
		 * });
		 */
		List<Employee> employee = cassandraTemplate.select(select, Employee.class);
		return employee;
	}
}
