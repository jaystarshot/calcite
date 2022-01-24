/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.benchmarks;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.schemata.bookstore.BookstoreSchema;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.UUID;

/**
 * Benchmarks JavaCC-generated SQL parser.
 */
//@Fork(value = 1, jvmArgsPrepend = "-Xmx128m")
//@Measurement(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
//@Warmup(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
//@State(Scope.Thread)
//@Threads(1)
//@BenchmarkMode(Mode.AverageTime)
//@OutputTimeUnit(TimeUnit.MICROSECONDS)

public class testBenchmarkOnSmall {

  @Param({ "1000" })
  int length;

  String sql;
  Planner p;

//  @Setup
  public void setup() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("select 1");
    Random rnd = new Random();
    rnd.setSeed(424242);
    length = 1000;
    for(int i=0; i < length/2;i++) {
      sb.append(", ");
      sb.append(String.format(" CASE WHEN authors.birthPlace.city > '%s' THEN authors.aid ELSE authors.name END ", UUID.randomUUID().toString()));
    }

    for(int i=0; i < length/2;i++) {
      sb.append(", ");
      sb.append(String.format(" CASE WHEN authors.aid > %d THEN authors.name ELSE authors.birthPlace.country END ", rnd.nextInt()));
    }
    sb.append(" FROM book.authors");

    sql = sb.toString();

    final SchemaPlus schema =
        Frameworks.createRootSchema(true).add("book",
            new ReflectiveSchema(new BookstoreSchema()));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
        .defaultSchema(schema)
        .programs(Programs.ofRules(Programs.RULE_SET))
        .build();
    p = Frameworks.getPlanner(config);
    parse();
//    RelNode r = p.rel(n).project();
//    plan = RelOptUtil.toString(r);
   }

//
//  @Benchmark
//  public SqlNode parseCached() throws SqlParseException {
//    return parser.parseQuery(sql);
//  }
//
//  @Benchmark
  public RelNode parse() throws SqlParseException, ValidationException, RelConversionException {
       SqlNode n = p.parse(sql);
       n = p.validate(n);
       RelNode rel = p.rel(n).project();
       p.close();
       p.reset();
       return  rel;
  }

  public static void main(String[] args) throws RunnerException{
    testBenchmarkOnSmall t = new testBenchmarkOnSmall();
     try {
       t.setup();
     } catch (Exception e) {
       e.printStackTrace();
     }
//    Options opt = new OptionsBuilder()
//        .include(testBenchmark.class.getSimpleName())
//        .addProfiler(GCProfiler.class)
//        .addProfiler(FlightRecorderProfiler.class)
//        .detectJvmArgs()
//        .build();
//
//    new Runner(opt).run();
}


//  public static void main(String[] args) throws RunnerException, SqlParseException {
////    Options opt = new OptionsBuilder()
////        .include(ParserBenchmark.class.getSimpleName())
////        .addProfiler(GCProfiler.class)
////        .addProfiler(FlightRecorderProfiler.class)
////        .detectJvmArgs()
////        .build();
////
////    new Runner(opt).run();
//
////    StringBuilder sb = new StringBuilder((int) (1000 * 1.2));
////    sb.append("select CASE WHEN TRUE THEN 1 ELSE 0 END");
//////    Random rnd = new Random();
//////    rnd.setSeed(424242);
//////    for (; sb.length() < 1000;) {
//////      sb.append(", ");
//////      sb.append(String.format("CAST(%s AS int)", String.valueOf(rnd.nextInt())));
//////    }
////    String sql = sb.toString();
////    SqlParser.create(sql).parseQuery();
//
//    String sql = "SELECT CASE WHEN authors.birthPlace.city > 'aa' THEN authors.aid ELSE authors.name END  FROM book.authors ";
//
//    final SchemaPlus schema =
//        Frameworks.createRootSchema(true).add("book",
//            new ReflectiveSchema(new BookstoreSchema()));
//
//    final FrameworkConfig config = Frameworks.newConfigBuilder()
//        .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
//        .defaultSchema(schema)
//        .programs(Programs.ofRules(Programs.RULE_SET))
//        .build();
//    String plan;
//    try (Planner p = Frameworks.getPlanner(config)) {
//      SqlNode n = p.parse(sql);
//      n = p.validate(n);
//      RelNode r = p.rel(n).project();
//      plan = RelOptUtil.toString(r);
//    } catch (RelConversionException e) {
//      e.printStackTrace();
//    } catch (ValidationException e) {
//      e.printStackTrace();
//    }
//  }

  }
