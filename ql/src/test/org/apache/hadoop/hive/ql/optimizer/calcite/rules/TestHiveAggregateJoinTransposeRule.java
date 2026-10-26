/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * HIVE-30154: pushing {@code SUM(DECIMAL)} through a join must not change the
 * result schema. Hive's COMPLETE-mode SUM widens DECIMAL precision by 10, so
 * the top SUM created by {@link HiveAggregateJoinTransposeRule} has to use that
 * physical type and cast back to the original aggregate type.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestHiveAggregateJoinTransposeRule {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new JavaTypeFactoryImpl(new HiveTypeSystemImpl());

  private RelOptPlanner planner;
  private RelOptCluster cluster;
  private final Map<List<String>, RelOptHiveTable> tables = new HashMap<>();

  @Before
  public void setUp() {
    HepProgramBuilder program = new HepProgramBuilder();
    program.addRuleInstance(
        new HiveAggregateJoinTransposeRule(new AtomicInteger(0), true, false));
    planner = new HepPlanner(program.build());
    cluster = RelOptCluster.create(planner, new RexBuilder(TYPE_FACTORY));
  }

  @Test
  public void testTopSplitDecimalSumMatchesHivePhysicalType() {
    RelDataType decimal72 = nullableDecimal(7, 2);
    RelDataType originalSumType = hiveSumType(decimal72);
    assertEquals("SUM(DECIMAL(7,2)) is DECIMAL(17,2)", 17, originalSumType.getPrecision());
    assertEquals(2, originalSumType.getScale());

    RelDataType partialSumType = originalSumType;
    RelDataType countType =
        TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true);
    RelDataType inputRowType = TYPE_FACTORY.builder()
        .add("partial_sum", partialSumType)
        .add("cnt", countType)
        .build();

    HiveSqlSumAggFunction sumFn = decimalSumFunction(originalSumType, decimal72);
    AggregateCall originalCall = AggregateCall.create(
        sumFn, false, ImmutableIntList.of(0), -1, originalSumType, "s");

    SqlSplittableAggFunction splitter = originalCall.getAggregation()
        .unwrap(SqlSplittableAggFunction.class);
    assertNotNull(splitter);

    List<RexNode> extra = new ArrayList<>();
    SqlSplittableAggFunction.Registry<RexNode> registry = node -> {
      int i = extra.indexOf(node);
      if (i < 0) {
        i = extra.size();
        extra.add(node);
      }
      return i;
    };

    AggregateCall topCall = splitter.topSplit(
        cluster.getRexBuilder(), registry, 0, inputRowType, originalCall, 0, 1);

    RelDataType hivePhysicalType = hiveSumType(extra.get(topCall.getArgList().get(0)).getType());
    assertEquals("Top SUM must use Hive COMPLETE-mode type so AST conversion does not widen again",
        hivePhysicalType.getPrecision(), topCall.getType().getPrecision());
    assertEquals(hivePhysicalType.getScale(), topCall.getType().getScale());
    assertEquals(27, topCall.getType().getPrecision());
    assertEquals(2, topCall.getType().getScale());

    RelDataType inferred = topCall.getAggregation().inferReturnType(
        TYPE_FACTORY, ImmutableList.of(extra.get(topCall.getArgList().get(0)).getType()));
    assertEquals("RelBuilder re-infers the top SUM from the agg function",
        hivePhysicalType.getPrecision(), inferred.getPrecision());
  }

  @Test
  public void testRulePreservesDecimalSumResultType() {
    RelDataType intType =
        TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true);
    RelDataType decimal72 = nullableDecimal(7, 2);
    RelDataType varcharType =
        TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 16), true);

    RelDataType factType = TYPE_FACTORY.builder()
        .add("k", intType)
        .add("v", decimal72)
        .build();
    RelDataType dimType = TYPE_FACTORY.builder()
        .add("k", intType)
        .add("name", varcharType)
        .build();

    registerTable("fact", factType, 1_000_000d);
    registerTable("dim", dimType, 1_000d);

    RelDataType originalSumType = hiveSumType(decimal72);
    HiveSqlSumAggFunction sumFn = decimalSumFunction(originalSumType, decimal72);

    RelBuilder b = createRelBuilder();
    RelNode plan = b
        .scan("fact")
        .scan("dim")
        .join(JoinRelType.INNER, b.equals(
            b.field(2, 0, 0),
            b.field(2, 1, 0)))
        .aggregate(
            b.groupKey(b.field(3)),
            b.aggregateCall(sumFn, b.field(1)).as("s"))
        .build();

    RelDataType originalType = plan.getRowType().getFieldList().get(1).getType();
    assertEquals(SqlTypeName.DECIMAL, originalType.getSqlTypeName());
    assertEquals(17, originalType.getPrecision());
    assertEquals(2, originalType.getScale());

    planner.setRoot(plan);
    RelNode optimized = unwrap(planner.findBestExp());

    assertNotEquals("HiveAggregateJoinTransposeRule should push the aggregate past the join:\n"
            + RelOptUtil.toString(plan),
        RelOptUtil.toString(plan), RelOptUtil.toString(optimized));

    RelDataType optimizedType = optimized.getRowType().getFieldList().get(1).getType();
    assertEquals("Transposed SUM(DECIMAL) must keep the original result type",
        originalType.getPrecision(), optimizedType.getPrecision());
    assertEquals(originalType.getScale(), optimizedType.getScale());
    assertEquals(SqlTypeName.DECIMAL, optimizedType.getSqlTypeName());

    assertDecimalSumsMatchHivePhysicalType(optimized);
  }

  private void assertDecimalSumsMatchHivePhysicalType(RelNode node) {
    RelNode current = unwrap(node);
    if (current instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) current;
      for (AggregateCall call : aggregate.getAggCallList()) {
        if (call.getAggregation().getKind() != SqlKind.SUM || call.getArgList().isEmpty()) {
          continue;
        }
        RelDataType inputType = aggregate.getInput().getRowType()
            .getFieldList().get(call.getArgList().get(0)).getType();
        if (inputType.getSqlTypeName() != SqlTypeName.DECIMAL) {
          continue;
        }
        RelDataType hiveType = hiveSumType(inputType);
        assertEquals("SUM(" + inputType + ") Calcite type must match Hive COMPLETE-mode inference "
                + hiveType + " so conversion through the AST cannot widen the column:\n"
                + RelOptUtil.toString(node),
            hiveType.getPrecision(), call.getType().getPrecision());
        assertEquals(hiveType.getScale(), call.getType().getScale());
      }
    }
    for (RelNode input : current.getInputs()) {
      assertDecimalSumsMatchHivePhysicalType(input);
    }
  }

  private static RelNode unwrap(RelNode node) {
    if (node instanceof HepRelVertex) {
      return unwrap(((HepRelVertex) node).getCurrentRel());
    }
    return node;
  }

  private static RelDataType nullableDecimal(int precision, int scale) {
    return TYPE_FACTORY.createTypeWithNullability(
        TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, precision, scale), true);
  }

  private static RelDataType hiveSumType(RelDataType inputType) {
    RelDataType derived = TYPE_FACTORY.getTypeSystem().deriveSumType(TYPE_FACTORY, inputType);
    return TYPE_FACTORY.createTypeWithNullability(derived, true);
  }

  private static HiveSqlSumAggFunction decimalSumFunction(RelDataType sumType, RelDataType inputType) {
    return new HiveSqlSumAggFunction(
        ReturnTypes.explicit(sumType),
        InferTypes.explicit(ImmutableList.of(inputType)),
        OperandTypes.NUMERIC);
  }

  private void registerTable(String name, RelDataType rowType, double rowCount) {
    RelOptHiveTable table = mock(RelOptHiveTable.class);
    doReturn(rowType).when(table).getRowType();
    doReturn(rowCount).when(table).getRowCount();
    doReturn(List.of(name)).when(table).getQualifiedName();
    LogicalTableScan scan = LogicalTableScan.create(cluster, table, Collections.emptyList());
    doReturn(scan).when(table).toRel(ArgumentMatchers.any());
    lenient().doReturn(mock(Table.class)).when(table).getHiveTableMD();
    tables.put(List.of(name), table);
  }

  private RelBuilder createRelBuilder() {
    RelOptSchema schema = mock(RelOptSchema.class);
    when(schema.getTableForMember(any())).thenAnswer(invocation -> {
      List<String> tableName = invocation.getArgument(0);
      RelOptHiveTable table = tables.get(tableName);
      if (table == null && tableName.size() == 1) {
        table = tables.get(List.of(tableName.get(0)));
      }
      return table;
    });
    return HiveRelFactories.HIVE_BUILDER.create(cluster, schema);
  }
}
