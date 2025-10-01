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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Arrow;
import org.apache.calcite.util.ArrowSet;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link BuiltInMetadata.FunctionalDependency} metadata handler
 * for the standard logical algebra.
 *
 * <p>Analyzes functional dependencies for relational operators using {@link ArrowSet}.
 *
 * <p>Key capabilities:
 * <ul>
 *   <li>Detects functional dependencies ({@link #determines}, {@link #determinesSet})</li>
 *   <li>Computes closure of attribute sets ({@link #computeClosure})</li>
 *   <li>Finds candidate keys or super keys ({@link #findCandidateKeysOrSuperKeys})</li>
 * </ul>
 *
 * @see Arrow
 * @see ArrowSet
 */
public class RelMdFunctionalDependency
    implements MetadataHandler<BuiltInMetadata.FunctionalDependency> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdFunctionalDependency(), BuiltInMetadata.FunctionalDependency.Handler.class);

  //~ Constructors -----------------------------------------------------------

  protected RelMdFunctionalDependency() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.FunctionalDependency> getDef() {
    return BuiltInMetadata.FunctionalDependency.DEF;
  }

  /**
   * Determines whether the specified column is functionally dependent on the given key.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param determinant Determinant column ordinal
   * @param dependent Dependent column ordinal
   * @return true if column is functionally dependent on key, false otherwise
   */
  public @Nullable Boolean determines(RelNode rel, RelMetadataQuery mq,
      int determinant, int dependent) {
    return determinesSet(rel, mq, ImmutableBitSet.of(determinant), ImmutableBitSet.of(dependent));
  }

  /**
   * Determines whether a set of columns functionally determines another set of columns.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param determinants Determinant column set
   * @param dependents Dependent column set
   * @return true if dependents are functionally determined by determinants, false otherwise
   */
  public Boolean determinesSet(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet determinants, ImmutableBitSet dependents) {
    ArrowSet fdSet = getFDs(rel, mq);
    return fdSet.implies(determinants, dependents);
  }

  /**
   * Computes the closure of a set of column ordinals under all functional dependencies.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param ordinals Column ordinals
   * @return Closure of the column ordinals
   */
  public ImmutableBitSet computeClosure(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet ordinals) {
    ArrowSet fdSet = getFDs(rel, mq);
    return fdSet.computeClosure(ordinals);
  }

  /**
   * Finds candidate keys or super keys within the specified attribute set.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param ordinals Column ordinals
   * @param onlyMinimalKeys Whether to return only minimal candidate keys
   * @return Set of candidate keys or super keys
   */
  public Set<ImmutableBitSet> findCandidateKeysOrSuperKeys(
      RelNode rel, RelMetadataQuery mq, ImmutableBitSet ordinals, boolean onlyMinimalKeys) {
    ArrowSet fdSet = getFDs(rel, mq);
    return fdSet.findCandidateKeysOrSuperKeys(ordinals, onlyMinimalKeys);
  }

  /**
   * Gets the functional dependency set for the specified relational node.
   * Dispatches to the appropriate handler based on node type.
   */
  public ArrowSet getFDs(RelNode rel, RelMetadataQuery mq) {
    rel = rel.stripped();
    if (rel instanceof TableScan) {
      return getTableScanFD((TableScan) rel);
    } else if (rel instanceof Project) {
      return getProjectFD((Project) rel, mq);
    } else if (rel instanceof Aggregate) {
      return getAggregateFD((Aggregate) rel, mq);
    } else if (rel instanceof Join) {
      return getJoinFD((Join) rel, mq);
    } else if (rel instanceof Calc) {
      return getCalcFD((Calc) rel, mq);
    } else if (rel instanceof Filter) {
      return getFilterFD((Filter) rel, mq);
    } else if (rel instanceof SetOp) {
      // TODO: Handle UNION, INTERSECT, EXCEPT functional dependencies
      return ArrowSet.EMPTY;
    } else if (rel instanceof Correlate) {
      // TODO: Handle CORRELATE functional dependencies
      return ArrowSet.EMPTY;
    }
    return getFD(rel.getInputs(), mq);
  }

  /**
   * Gets the functional dependencies of input nodes.
   * For multi-input nodes without specific logic, returns an empty set.
   */
  private ArrowSet getFD(List<RelNode> inputs, RelMetadataQuery mq) {
    if (inputs.size() != 1) {
      // Conservative approach for multi-input nodes without specific logic
      return ArrowSet.EMPTY;
    }
    return getFDs(inputs.get(0), mq);
  }

  /**
   * Gets the functional dependencies for a TableScan node.
   * The table's primary key determines all other columns.
   */
  private static ArrowSet getTableScanFD(TableScan rel) {
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();

    RelOptTable table = rel.getTable();
    List<ImmutableBitSet> keys = table.getKeys();
    if (keys == null || keys.isEmpty()) {
      return fdBuilder.build();
    }

    for (ImmutableBitSet key : keys) {
      ImmutableBitSet allColumns = ImmutableBitSet.range(rel.getRowType().getFieldCount());
      ImmutableBitSet dependents = allColumns.except(key);
      if (!dependents.isEmpty()) {
        fdBuilder.addArrow(key, dependents);
      }
    }

    return fdBuilder.build();
  }

  /**
   * Gets the functional dependencies for a Project node.
   * Maps input dependencies through projection expressions.
   */
  private ArrowSet getProjectFD(Project rel, RelMetadataQuery mq) {
    return getProjectionFD(rel.getInput(), rel.getProjects(), mq);
  }

  /**
   * Computes the functional dependencies for projection operations (Project/Calc).
   *
   * @param input Input relation
   * @param projections List of projection expressions
   * @param mq Metadata query
   * @return Functional dependency set after projection
   */
  private ArrowSet getProjectionFD(
      RelNode input, List<RexNode> projections, RelMetadataQuery mq) {
    ArrowSet inputFdSet = getFDs(input, mq);
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();

    // Create mapping from input column indices to project column indices
    Mappings.TargetMapping inputToOutputMap =
        RelOptUtil.permutation(projections, input.getRowType()).inverse();

    // Map input functional dependencies to project dependencies
    mapInputFDs(inputFdSet, inputToOutputMap, fdBuilder);

    int fieldCount = projections.size();
    final ImmutableBitSet[] inputBits = new ImmutableBitSet[fieldCount];
    final Map<RexNode, Integer> uniqueExprToIndex = new HashMap<>();
    final Map<RexLiteral, Integer> literalToIndex = new HashMap<>();
    final Map<RexInputRef, Integer> refToIndex = new HashMap<>();
    for (int i = 0; i < fieldCount; i++) {
      RexNode expr = projections.get(i);

      // Skip non-deterministic expressions
      if (!RexUtil.isDeterministic(expr)) {
        continue;
      }

      // Handle identical expressions in the projection list
      Integer prev = uniqueExprToIndex.putIfAbsent(expr, i);
      if (prev != null) {
        fdBuilder.addBidirectionalArrow(prev, i);
      }

      // Track literal constants to handle them specially
      if (expr instanceof RexLiteral) {
        literalToIndex.put((RexLiteral) expr, i);
        continue;
      }

      if (expr instanceof RexInputRef) {
        refToIndex.put((RexInputRef) expr, i);
        inputBits[i] = ImmutableBitSet.of(((RexInputRef) expr).getIndex());
      }

      inputBits[i] = RelOptUtil.InputFinder.bits(expr);
    }

    // Remove literals from uniqueExprToIndex to avoid redundant processing
    uniqueExprToIndex.keySet().removeIf(key -> key instanceof RexLiteral);

    for (Map.Entry<RexNode, Integer> entry : uniqueExprToIndex.entrySet()) {
      RexNode expr = entry.getKey();
      Integer i = entry.getValue();

      // All columns determine literals
      literalToIndex.values()
          .forEach(l -> fdBuilder.addArrow(i, l));

      // Input columns determine identical expressions
      refToIndex.forEach((k, v) -> {
        ImmutableBitSet refIndex = ImmutableBitSet.of(k.getIndex());
        ImmutableBitSet bitSet = expr instanceof RexInputRef
            ? ImmutableBitSet.of(((RexInputRef) expr).getIndex())
            : inputBits[i];
        if (inputFdSet.implies(refIndex, bitSet)) {
          fdBuilder.addArrow(v, i);
        }
      });
    }

    return fdBuilder.build();
  }

  /**
   * Maps input functional dependencies to output dependencies based on column mapping.
   */
  private static void mapInputFDs(ArrowSet inputFdSet,
      Mappings.TargetMapping mapping, ArrowSet.Builder outputFdBuilder) {
    for (Arrow inputFd : inputFdSet.getArrows()) {
      ImmutableBitSet determinants = inputFd.getDeterminants();
      ImmutableBitSet dependents = inputFd.getDependents();

      // Map all determinant columns
      ImmutableBitSet mappedDeterminants = mapAllCols(determinants, mapping);
      if (mappedDeterminants.isEmpty()) {
        continue;
      }

      // Map only the dependent columns that can be mapped
      ImmutableBitSet mappedDependents = mapAvailableCols(dependents, mapping);
      if (!mappedDependents.isEmpty()) {
        outputFdBuilder.addArrow(mappedDeterminants, mappedDependents);
      }
    }
  }

  /**
   * Maps all column ordinals in the set. Returns an empty set if any column cannot be mapped.
   */
  private static ImmutableBitSet mapAllCols(
      ImmutableBitSet ordinals, Mappings.TargetMapping mapping) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int ord : ordinals) {
      int mappedOrd = mapping.getTargetOpt(ord);
      if (mappedOrd < 0) {
        return ImmutableBitSet.of();
      }
      builder.set(mappedOrd);
    }
    return builder.build();
  }

  /**
   * Maps only the column ordinals that can be mapped, ignoring unmappable ones.
   */
  private static ImmutableBitSet mapAvailableCols(
      ImmutableBitSet ordinals, Mappings.TargetMapping mapping) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int ord : ordinals) {
      int mappedOrd = mapping.getTargetOpt(ord);
      if (mappedOrd >= 0) {
        builder.set(mappedOrd);
      }
    }
    return builder.build();
  }

  /**
   * Gets the functional dependencies for an Aggregate node.
   * Group keys determine all aggregate columns.
   * Preserves input dependencies that only involve group columns.
   */
  private ArrowSet getAggregateFD(Aggregate rel, RelMetadataQuery mq) {
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();
    ArrowSet inputFdSet = getFDs(rel.getInput(), mq);

    ImmutableBitSet groupSet = rel.getGroupSet();

    // Preserve input FDs that only involve group columns
    if (Aggregate.isSimple(rel)) {
      for (Arrow inputFd : inputFdSet.getArrows()) {
        ImmutableBitSet determinants = inputFd.getDeterminants();
        ImmutableBitSet dependents = inputFd.getDependents();

        // Only preserve if both determinants and dependents are within group columns
        if (groupSet.contains(determinants) && groupSet.contains(dependents)) {
          fdBuilder.addArrow(determinants, dependents);
        }
      }
    }

    // Group keys determine all aggregate columns
    if (!groupSet.isEmpty() && !rel.getAggCallList().isEmpty()) {
      for (int i = rel.getGroupCount(); i < rel.getRowType().getFieldCount(); i++) {
        fdBuilder.addArrow(groupSet, ImmutableBitSet.of(i));
      }
    }

    return fdBuilder.build();
  }

  /**
   * Gets the functional dependencies for a Filter node.
   * Extracts equality dependencies from filter conditions and merges with input dependencies.
   */
  private ArrowSet getFilterFD(Filter rel, RelMetadataQuery mq) {
    ArrowSet inputSet = getFDs(rel.getInput(), mq);
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();
    addFDsFromEqualityCondition(rel.getCondition(), fdBuilder);
    return fdBuilder.build().union(inputSet);
  }

  /**
   * Gets the functional dependencies for a Join node.
   * Handles dependency preservation and cross-table dependencies based on join type.
   */
  private ArrowSet getJoinFD(Join rel, RelMetadataQuery mq) {
    ArrowSet leftFdSet = getFDs(rel.getLeft(), mq);
    ArrowSet rightFdSet = getFDs(rel.getRight(), mq);

    int leftFieldCount = rel.getLeft().getRowType().getFieldCount();
    JoinRelType joinType = rel.getJoinType();

    switch (joinType) {
    case INNER:
    case LEFT:
    case RIGHT:
      ArrowSet.Builder joinFdBuilder = new ArrowSet.Builder()
          .addArrowSet(leftFdSet.union(shiftFdSet(rightFdSet, leftFieldCount)));
      addFDsFromEqualityCondition(rel.getCondition(), joinFdBuilder);
      return joinFdBuilder.build();
    case SEMI:
    case ANTI:
      return leftFdSet.clone();
    default:
      return ArrowSet.EMPTY;
    }
  }

  /**
   * Gets the functional dependencies for a Calc node.
   * Maps input dependencies through projection expressions.
   */
  private ArrowSet getCalcFD(Calc rel, RelMetadataQuery mq) {
    List<RexNode> projections = rel.getProgram().expandList(rel.getProgram().getProjectList());
    return getProjectionFD(rel.getInput(), projections, mq);
  }

  /**
   * Shifts column indices in the dependency set (used for right table dependencies in joins).
   *
   * @param fdSet Dependency set
   * @param offset Index offset
   * @return Dependency set with shifted indices
   */
  private ArrowSet shiftFdSet(ArrowSet fdSet, int offset) {
    ArrowSet.Builder shiftedFdSetBuilder = new ArrowSet.Builder();
    for (Arrow fd : fdSet.getArrows()) {
      ImmutableBitSet shiftedDeterminants = fd.getDeterminants().shift(offset);
      ImmutableBitSet shiftedDependents = fd.getDependents().shift(offset);
      shiftedFdSetBuilder.addArrow(shiftedDeterminants, shiftedDependents);
    }
    return shiftedFdSetBuilder.build();
  }

  /**
   * Extracts functional dependencies from equality and AND conditions.
   * Supports col1 = col2, col1 IS NOT DISTINCT FROM col2, and compound AND conditions.
   */
  private static void addFDsFromEqualityCondition(RexNode condition, ArrowSet.Builder builder) {
    if (!(condition instanceof RexCall)) {
      return;
    }

    RexCall call = (RexCall) condition;
    if (call.getOperator().getKind() == SqlKind.EQUALS
        || call.getOperator().getKind() == SqlKind.IS_NOT_DISTINCT_FROM) {
      List<RexNode> operands = call.getOperands();
      if (operands.size() == 2) {
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        if (left instanceof RexInputRef && right instanceof RexInputRef) {
          int leftRef = ((RexInputRef) left).getIndex();
          int rightRef = ((RexInputRef) right).getIndex();

          builder.addBidirectionalArrow(leftRef, rightRef);
        }
      }
    } else if (call.getOperator().getKind() == SqlKind.AND) {
      for (RexNode operand : call.getOperands()) {
        addFDsFromEqualityCondition(operand, builder);
      }
    }
  }
}
