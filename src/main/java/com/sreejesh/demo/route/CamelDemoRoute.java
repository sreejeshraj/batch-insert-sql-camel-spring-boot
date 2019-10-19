package com.sreejesh.demo.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import com.github.javafaker.Faker;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Component
@Data
@EqualsAndHashCode(callSuper = true)

public class CamelDemoRoute extends RouteBuilder {

		
	@Override
	public void configure() throws Exception {

		// @formatter:off
		
		
		
		//SELECT name FROM person WHERE id=1;
		
		//Table creation script
		/*
		
		CREATE TABLE `person` (
				`id` INT(11) NOT NULL AUTO_INCREMENT,
				`Name` VARCHAR(50) NOT NULL,
				PRIMARY KEY (`id`)
			)
			COLLATE='latin1_swedish_ci'
			ENGINE=InnoDB
			AUTO_INCREMENT=12
			;
		*/
						
		from("timer://dbQueryTimer?period=10s&repeatCount=1")
		.routeId("SqlPaginationRoute")
		.setBody(() -> {return new ArrayList<Map<String,String>>();})
		.loop(10)
		.process(new InsertSqlQueryParameterSetterByCallingJavaFakerProcessor())
		//.to("sql:SELECT name FROM person limit :#startRowNum,:#pageSize?dataSource=#dataSource")
		.log("******STEP 20: Database query executed - body:${body}******")
		.end()
		.to("sql:INSERT INTO person(Name) VALUES (:#personName)?dataSource=#dataSource&batch=true")
		.log("******STEP 100: Batch Insert Completed!!! - Update count:${header.CamelSqlUpdateCount}\n${body}******")
		;
		
		
		// @formatter:on

	}
	
	
	private final class InsertSqlQueryParameterSetterByCallingJavaFakerProcessor implements Processor {
		@Override
		public void process(Exchange exchange) throws Exception {
			List<Map<String,String>> listOfMaps = exchange.getIn().getBody(List.class);
			Objects.requireNonNull(listOfMaps,"listOfMaps cannot be empty!");
			Map<String,String> sqlQueryParameterMap = new HashMap<>();
			Faker faker = new Faker();
			String personName = faker.name().name();
			log.info("Step 10 - personName:{}",personName);
			sqlQueryParameterMap.put("personName", personName);
			listOfMaps.add(sqlQueryParameterMap);
			//No need to set listOfMaps as body. It is already set
			//exchange.getIn().setBody(listOfMaps);
		}
	}
	
}
