package com.cassandra.CassandraRestApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages="com.cassandra.CassandraRestApp")
public class CassandraRestApplication {

	public static void main(String[] args) {
		SpringApplication.run(CassandraRestApplication.class, args);
	}
}
