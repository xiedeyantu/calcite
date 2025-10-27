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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Implementation of {@link RelCollation} that stores both optimized and original collations.
 *
 * <p>This class is used when collations are optimized based on functional dependencies.
 * For example, if a sort is on [0, 1] but column 1 is functionally determined by column 0,
 * the collation can be optimized to [0]. This class stores both the optimized collation [0]
 * and the original collation [0, 1] so that trait satisfaction checks work correctly.
 *
 * <p>Example:
 * <pre>{@code
 * // Original collation: [deptno, empid]
 * // After optimization (empid determined by deptno): [deptno]
 * OptimizedRelCollationImpl optimized = new OptimizedRelCollationImpl(
 *     ImmutableList.of(new RelFieldCollation(0)),  // optimized
 *     ImmutableList.of(new RelFieldCollation(0), new RelFieldCollation(1)));  // original
 * // Can satisfy both [0] and [0, 1]
 * optimized.satisfies(RelCollations.of(0));      // true
 * optimized.satisfies(RelCollations.of(0, 1));   // true
 * }</pre>
 */
public class OptimizedRelCollationImpl extends RelCollationImpl {
  //~ Instance fields --------------------------------------------------------

  /**
   * The original field collations before optimization.
   * This is used for trait satisfaction checks.
   */
  private final ImmutableList<RelFieldCollation> originalFieldCollations;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an optimized collation.
   *
   * @param fieldCollations The optimized field collations
   * @param originalFieldCollations The original field collations before optimization
   */
  protected OptimizedRelCollationImpl(
      ImmutableList<RelFieldCollation> fieldCollations,
      ImmutableList<RelFieldCollation> originalFieldCollations) {
    super(fieldCollations);
    this.originalFieldCollations = originalFieldCollations;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public List<RelFieldCollation> getOriginalCollations() {
    return originalFieldCollations;
  }

  @Override public RelCollationImpl apply(
      org.apache.calcite.util.mapping.Mappings.TargetMapping mapping) {
    // Apply mapping to both optimized and original collations
    List<RelFieldCollation> newFieldCollations =
        org.apache.calcite.rex.RexUtil.applyFields(mapping, getFieldCollations());
    List<RelFieldCollation> newOriginalFieldCollations =
        org.apache.calcite.rex.RexUtil.applyFields(mapping, originalFieldCollations);

    // If nothing changed, return this
    if (newFieldCollations.equals(getFieldCollations())
        && newOriginalFieldCollations.equals(originalFieldCollations)) {
      return this;
    }

    // Create a new collation with mapped collations
    // ofOptimized will return OptimizedRelCollationImpl if fields differ,
    // or plain RelCollationImpl if they're the same (both are subclass of RelCollationImpl)
    return (RelCollationImpl) RelCollations.ofOptimized(
        newFieldCollations,
        newOriginalFieldCollations);
  }

  @Override public boolean satisfies(RelTrait trait) {
    if (this == trait) {
      return true;
    }
    if (!(trait instanceof RelCollationImpl)) {
      return false;
    }

    final RelCollationImpl that = (RelCollationImpl) trait;

    // First, try the simple prefix matching with optimized collations
    if (Util.startsWith(getFieldCollations(), that.getFieldCollations())) {
      return true;
    }

    // If simple prefix matching fails, check if original collations satisfy the requirement
    // For example: original [0, 1] optimized to [0], and we need to satisfy [0, 1].
    // The original [0, 1] prefix-matches [0, 1], so this returns true.
    if (Util.startsWith(originalFieldCollations, that.getFieldCollations())) {
      return true;
    }

    return false;
  }

  @Override public int hashCode() {
    // MUST be consistent with equals() to satisfy Java contract.
    // Include both optimized and original field collations.
    return java.util.Objects.hash(getFieldCollations(), originalFieldCollations);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    // OptimizedRelCollationImpl is NEVER equal to regular RelCollationImpl.
    // Two OptimizedRelCollationImpl are equal if both their optimized and
    // original field collations are equal.
    if (obj instanceof OptimizedRelCollationImpl) {
      OptimizedRelCollationImpl that = (OptimizedRelCollationImpl) obj;
      return this.getFieldCollations().equals(that.getFieldCollations())
          && this.originalFieldCollations.equals(that.originalFieldCollations);
    }
    return false;
  }
}
