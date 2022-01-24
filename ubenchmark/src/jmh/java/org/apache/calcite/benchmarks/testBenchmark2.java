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
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks JavaCC-generated SQL parser.
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx128m")
@Measurement(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 7, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class testBenchmark2 {

  @Param({ "1000" })
  int length;

  String sql;
  Planner p;

  @Setup
  public void setup() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("select 1");
    Random rnd = new Random();
    rnd.setSeed(424242);
    for(int i=0; i < length;i++) {
      sb.append(", ");
      sb.append(String.format(" tests.aid%s / CASE WHEN tests.aid%s > %d THEN tests.aid%s ELSE tests.aid%s  END ",String.valueOf(rnd.nextInt(250))
          , String.valueOf(i%250), rnd.nextInt(250),
          String.valueOf(rnd.nextInt(250)), String.valueOf(rnd.nextInt(250)))
      );
    }
    sb.append(" FROM test1.tests");

    sql = sb.toString();

    final SchemaPlus schema =
        Frameworks.createRootSchema(true).add("test1",
            new ReflectiveSchema(new test1()));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
        .defaultSchema(schema)
        .programs(Programs.ofRules(Programs.RULE_SET))
        .build();
    p = Frameworks.getPlanner(config);
  }

  @Benchmark
  public RelNode parse() throws SqlParseException, ValidationException, RelConversionException {
    SqlNode n = p.parse(sql);
    n = p.validate(n);
    RelNode rel = p.rel(n).project();
    p.close();
    p.reset();
    return  rel;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(testBenchmark2.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .addProfiler(FlightRecorderProfiler.class)
        .detectJvmArgs()
        .build();

      new Runner(opt).run();
  }

}
