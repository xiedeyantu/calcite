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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Planner rule that removes a {@link Join left-join and right-join} from a join
 * tree.
 *
 * @see CoreRules#SEMI_JOIN_REMOVE
 */
@Value.Enclosing
public class OuterJoinRemoveRule
    extends RelRule<OuterJoinRemoveRule.Config>
    implements TransformationRule {

  /** Creates a OuterJoinRemoveRule. */
  protected OuterJoinRemoveRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(1);
    if (join.getCondition().isAlwaysFalse()) {
      removeJoin(call);
      return;
    }
    config.matchHandler().accept(this, call);
  }

  private static void matchJoin(OuterJoinRemoveRule rule, RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join join = call.rel(1);

  }

  private static void matchProjectJoin(OuterJoinRemoveRule rule, RelOptRuleCall call) {
    Project project = call.rel(0);
    Join join = call.rel(1);
  }

  private static void matchAggregateJoin(OuterJoinRemoveRule rule, RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    Join join = call.rel(1);
  }


  private static JoinRelType inferJoinType(JoinRelType joinType,
      boolean leftHasNonNullPredicate, boolean rightHasNonNullPredicate) {
    switch (joinType) {
    case RIGHT:
      if (leftHasNonNullPredicate) {
        return JoinRelType.INNER;
      }
      break;
    case LEFT:
      if (rightHasNonNullPredicate) {
        return JoinRelType.INNER;
      }
      break;
    case FULL:
      if (leftHasNonNullPredicate && rightHasNonNullPredicate) {
        return JoinRelType.INNER;
      } else if (leftHasNonNullPredicate) {
        return JoinRelType.LEFT;
      } else if (rightHasNonNullPredicate) {
        return JoinRelType.RIGHT;
      }
      break;
    default:
      break;
    }
    return joinType;
  }


  private static void removeJoin(RelOptRuleCall call) {
    Join join = call.rel(0);

    final RelBuilder builder = call.builder();
    final boolean isLeftJoin = join.getJoinType() == JoinRelType.LEFT;

    RelNode input = isLeftJoin ? join.getLeft() : join.getRight();
    builder.push(input);
    List<RexNode> fields = new ArrayList<>(builder.fields());
    List<RexNode> nulls = new ArrayList<>();
    RelNode inputWithNull = isLeftJoin ? join.getRight() : join.getLeft();
    for (int i = 0; i < inputWithNull.getRowType().getFieldCount(); i++) {
      nulls.add(
          builder.getRexBuilder().makeNullLiteral(
              inputWithNull.getRowType().getFieldList().get(i).getType()));
    }

    List<RexNode> projects = isLeftJoin
        ? Stream.concat(fields.stream(), nulls.stream()).collect(Collectors.toList())
        : Stream.concat(nulls.stream(), fields.stream()).collect(Collectors.toList());

    builder.project(projects);

    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableOuterJoinRemoveRule.Config.builder()
        .withMatchHandler(OuterJoinRemoveRule::matchJoin)
        .build()
        .withDescription("FilterJoinEliminatorRule")
        .withOperandFor(RelNode.class, Join.class);

    Config PROJECT_JOIN = ImmutableOuterJoinRemoveRule.Config.builder()
        .withMatchHandler(OuterJoinRemoveRule::matchProjectJoin)
        .build()
        .withDescription("ProjectJoinEliminatorRule")
        .withOperandFor(Project.class, Join.class);

    Config AGGREGATE_JOIN = ImmutableOuterJoinRemoveRule.Config.builder()
        .withMatchHandler(OuterJoinRemoveRule::matchAggregateJoin)
        .build()
        .withDescription("AggregateJoinEliminatorRule")
        .withOperandFor(Aggregate.class, Join.class);


    @Override default OuterJoinRemoveRule toRule() {
      return new OuterJoinRemoveRule(this);
    }

    @Value.Parameter
    MatchHandler<OuterJoinRemoveRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    OuterJoinRemoveRule.Config withMatchHandler(MatchHandler<OuterJoinRemoveRule> matchHandler);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends RelNode> anyClass,
        Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
          b0.operand(anyClass).oneInput(b1 ->
              b1.operand(joinClass).predicate(join ->
                  join.getJoinType() == JoinRelType.LEFT
                      || join.getJoinType() == JoinRelType.RIGHT).anyInputs()))
          .as(Config.class);
    }
  }
}
