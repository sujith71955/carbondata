/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.testsuite.detailquery

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.Row


/**
 * Test Class for verifying NO_DICTIONARY_COLUMN feature.
 * @author S71955
 *
 */
class NO_DICTIONARY_COL_TestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    //For the Hive table creation and data loading
    sql("create table NO_DICTIONARY_HIVE_6(empno int,empname string,designation string,doj Timestamp,workgroupcategory int, workgroupcategoryname string,deptno int, deptname string, projectcode int, projectjoindate Timestamp,projectenddate Timestamp,attendance int,utilization int,salary int) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by ':'")
    sql("load data local inpath './src/test/resources/data.csv' into table NO_DICTIONARY_HIVE_6");
    //For Carbon cube creation.
    sql("CREATE CUBE NO_DICTIONARY_CARBON_6 DIMENSIONS (empno Integer, empname String, " +
      "designation String, doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, " +
      "deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp, " +
      "projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,salary Integer) " + "OPTIONS (NO_DICTIONARY(empno,empname,deptname)PARTITIONER [PARTITION_COUNT=1])").show()
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE NO_DICTIONARY_CARBON_6 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");

  }
  test("Detail Query with NO_DICTIONARY_COLUMN Compare With HIVE RESULT") {
    
  
    checkAnswer(
      sql("select empno from NO_DICTIONARY_CARBON_6"),
      Seq(Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row(17), Row(18), Row(19), Row(20)))
      
     
  }
  
   test("Filter Query with NO_DICTIONARY_COLUMN Compare With HIVE RESULT") {
    
     checkAnswer(
      sql("select empno from NO_DICTIONARY_HIVE_6 where empno=15"),
      sql("select empno from NO_DICTIONARY_CARBON_6 where empno=15"))
   }
   
    test("Filter Query with NO_DICTIONARY_COLUMN and DICTIONARY_COLUMN Compare With HIVE RESULT") {
    
     checkAnswer(
      sql("select empno from NO_DICTIONARY_HIVE_6 where empno=15 and deptno=12"),
      sql("select empno from NO_DICTIONARY_CARBON_6 where empno=15 and deptno=12"))
   }
    
    test("Distinct Query with NO_DICTIONARY_COLUMN  Compare With HIVE RESULT") {
    
     checkAnswer(
      sql("select count(distinct empno) from NO_DICTIONARY_HIVE_6"),
      sql("select count(distinct empno) from NO_DICTIONARY_CARBON_6"))
   }
    
     
    
    

  override def afterAll {
   //sql("drop cube NO_DICTIONARY_HIVE_1")
   //sql("drop cube NO_DICTIONARY_CARBON_1")
  }
}