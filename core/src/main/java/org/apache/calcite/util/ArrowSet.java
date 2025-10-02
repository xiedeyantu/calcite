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
package org.apache.calcite.util;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Represents a set of functional dependencies.
 * Each functional dependency is represented as an {@link Arrow} from a set of
 * determinant columns to a set of dependent columns.
 *
 * <p>An {@link ArrowSet} captures the complete set of functional dependencies
 * that hold in a relation. Provides core algorithms in functional dependency theory.
 *
 * @see Arrow
 * @see ImmutableBitSet
 */
public class ArrowSet {
  public static final ArrowSet EMPTY = new ArrowSet(ImmutableSet.of());

  // Maps each determinant set to the dependent set it functionally determines.
  private final Map<ImmutableBitSet, ImmutableBitSet> dependencyGraph = new HashMap<>();

  // Maps each column ordinal to its containing determinant sets for efficient reverse lookup.
  private final Map<Integer, Set<ImmutableBitSet>> reverseIndex = new HashMap<>();

  // All arrows in this ArrowSet.
  private final Set<Arrow> arrowSet = new HashSet<>();

  public ArrowSet(Set<Arrow> arrows) {
    arrowSet.addAll(arrows);
    for (Arrow arrow : arrows) {
      ImmutableBitSet determinants = arrow.getDeterminants();
      ImmutableBitSet dependents = arrow.getDependents();
      dependencyGraph.merge(determinants, dependents, ImmutableBitSet::union);
      for (int attr : determinants) {
        reverseIndex.computeIfAbsent(attr, k -> new HashSet<>()).add(determinants);
      }
    }
  }

  public Set<ImmutableBitSet> getDeterminants(int attr) {
    return reverseIndex.getOrDefault(attr, ImmutableSet.of());
  }

  public ImmutableBitSet getDependents(ImmutableBitSet determinants) {
    return dependencyGraph.getOrDefault(determinants, ImmutableBitSet.of());
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Computes the closure of a given set of column ordinals with respect to the current
   * collection of functional dependencies.
   *
   * <p>The closure of a set of ordinals is defined as the set of all column ordinals
   * that can be functionally determined from the specified set by applying zero or more
   * functional dependencies present in this collection. This concept is fundamental
   * in identifying keys, candidate keys, and in the normalization of relational schemas.
   *
   * <p>Example:
   * <blockquote>
   * <pre>
   * // Given functional dependencies:
   * {0} → {1}
   * {1} → {2}
   *
   * // Closure computation:
   * closure({0}) = {0, 1, 2}
   * </pre>
   * </blockquote>
   *
   * <p>Explanation:
   * <ul>
   *   <li>Starting with {0}
   *   <li>Apply {0} → {1}: set becomes {0, 1}
   *   <li>Apply {1} → {2}: set becomes {0, 1, 2}
   *   <li>No more dependencies can be applied → closure is {0, 1, 2}
   * </ul>
   *
   * @param ordinals the set of column ordinals whose closure is to be computed
   * @return an immutable set of column ordinals representing the closure of ordinals
   *         under the current set of functional dependencies
   */
  public ImmutableBitSet computeClosure(ImmutableBitSet ordinals) {
    if (ordinals.isEmpty()) {
      return ImmutableBitSet.of();
    }

    ImmutableBitSet closure = ordinals;
    Set<Integer> processed = new HashSet<>(ordinals.asList());
    Queue<Integer> queue = new ArrayDeque<>(ordinals.asList());

    while (!queue.isEmpty()) {
      Integer currentAttr =
          requireNonNull(queue.poll(), "Queue returned null while computing closure");
      Set<ImmutableBitSet> relatedDeterminants = getDeterminants(currentAttr);
      for (ImmutableBitSet determinants : relatedDeterminants) {
        if (closure.contains(determinants)) {
          ImmutableBitSet dependents = getDependents(determinants);
          ImmutableBitSet newOrdinals = dependents.except(closure);
          if (!newOrdinals.isEmpty()) {
            closure = closure.union(newOrdinals);
            for (int ordinal : newOrdinals) {
              if (!processed.contains(ordinal)) {
                queue.add(ordinal);
                processed.add(ordinal);
              }
            }
          }
        }
      }
    }
    return closure;
  }

  /**
   * Finds all candidate keys or superkeys for a relation based on the current set of
   * functional dependencies.
   *
   * <p>A <b>candidate key</b> is a minimal set of ordinals that functionally determines
   * all other ordinals in the relation.
   *
   * <p>A <b>superkey</b> is any superset of a candidate key that also determines all ordinals.
   *
   * <p>Example:
   * <blockquote>
   * <pre>
   * // Given functional dependencies:
   * {0} → {1}
   * {1} → {2}
   * {2} → {3}
   *
   * // For relation with ordinals {0, 1, 2, 3}:
   * Candidate keys: [{0}]  // since {0}⁺ = {0, 1, 2, 3}
   * Superkeys: [{0}, {0,1}, {0,2}, {0,3}, {0,1,2}, ...]
   * </pre>
   * </blockquote>
   *
   * <p>When {@code onlyMinimalKeys} is true, the method returns only minimal
   * candidate keys. When false, it may return all superkeys (which includes
   * candidate keys and their supersets).
   *
   * @param ordinals the complete set of attribute indices in the relation schema
   * @param onlyMinimalKeys if true, returns only minimal candidate keys;
   *                        if false, returns all superkeys
   * @return a set of candidate keys or superkeys, each represented as an {@link ImmutableBitSet}
   *
   * @see #computeClosure(ImmutableBitSet)
   */
  public Set<ImmutableBitSet> findCandidateKeysOrSuperKeys(
      ImmutableBitSet ordinals, boolean onlyMinimalKeys) {
    if (dependencyGraph.isEmpty()) {
      return ImmutableSet.of(ordinals);
    }

    ImmutableBitSet nonDependentOrdinals = findNonDependentAttributes(ordinals);
    if (computeClosure(nonDependentOrdinals).contains(ordinals)) {
      return ImmutableSet.of(nonDependentOrdinals);
    }

    Set<ImmutableBitSet> result = new HashSet<>();
    int minKeySize = Integer.MAX_VALUE;
    PriorityQueue<ImmutableBitSet> queue =
        new PriorityQueue<>(Comparator.comparingInt(ImmutableBitSet::cardinality));
    Set<ImmutableBitSet> visited = new HashSet<>();
    queue.add(nonDependentOrdinals);
    while (!queue.isEmpty()) {
      ImmutableBitSet keys = requireNonNull(queue.poll(), "queue.poll() returned null");
      if (visited.contains(keys)) {
        continue;
      }
      visited.add(keys);
      if (onlyMinimalKeys && keys.cardinality() > minKeySize) {
        break;
      }
      boolean covered = false;
      for (ImmutableBitSet key : result) {
        if (keys.contains(key)) {
          covered = true;
          break;
        }
      }
      if (covered) {
        continue;
      }
      ImmutableBitSet keysClosure = computeClosure(keys);
      if (keysClosure.contains(ordinals)) {
        result.add(keys);
        if (onlyMinimalKeys) {
          minKeySize = keys.cardinality();
        }
        continue;
      }
      ImmutableBitSet remain = ordinals.except(keys);
      for (int attr : remain) {
        if (keysClosure.get(attr)) {
          continue;
        }
        ImmutableBitSet next = keys.union(ImmutableBitSet.of(attr));
        if (!visited.contains(next)) {
          queue.add(next);
        }
      }
    }
    return result.isEmpty() ? ImmutableSet.of(ordinals) : result;
  }

  /**
   * Find ordinals in the given set that never appear as dependents in any functional dependency.
   * These are the "source" ordinals that cannot be derived from others.
   */
  private ImmutableBitSet findNonDependentAttributes(ImmutableBitSet ordinals) {
    ImmutableBitSet dependentsAttrs = dependencyGraph.values().stream()
        .reduce(ImmutableBitSet.of(), ImmutableBitSet::union);
    return ordinals.except(dependentsAttrs);
  }

  /**
   * Returns a new ArrowSet that is the union of this and another ArrowSet.
   */
  public ArrowSet union(ArrowSet other) {
    Set<Arrow> unionSet = new HashSet<>();
    unionSet.addAll(this.getArrows());
    unionSet.addAll(other.getArrows());
    return new ArrowSet(unionSet);
  }

  /**
   * Returns all arrows (functional dependencies) in this ArrowSet.
   *
   * @return a set of Arrow objects representing all functional dependencies
   */
  public Set<Arrow> getArrows() {
    return arrowSet;
  }

  @Override public ArrowSet clone() {
    return new ArrowSet(new HashSet<>(this.arrowSet));
  }

  public boolean equalTo(ArrowSet other) {
    for (Map.Entry<ImmutableBitSet, ImmutableBitSet> entry : dependencyGraph.entrySet()) {
      if (!other.implies(entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    for (Map.Entry<ImmutableBitSet, ImmutableBitSet> entry : other.dependencyGraph.entrySet()) {
      if (!implies(entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ArrowSet{");
    boolean first = true;
    for (Arrow arrow : getArrows()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(arrow);
      first = false;
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Returns true if this ArrowSet implies that determinants determine dependents.
   * That is, if dependents ⊆ closure(determinants).
   */
  public boolean implies(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    ImmutableBitSet dets = dependencyGraph.get(determinants);
    if (dets != null && dets.contains(dependents)) {
      return true;
    }
    return computeClosure(determinants).contains(dependents);
  }

  /**
   * Builder for ArrowSet.
   * Supports fluent Arrow addition and builds the dependency graph.
   */
  public static class Builder {
    private final Set<Arrow> arrowSet = new HashSet<>();

    /**
     * Add an Arrow from determinant set to dependent set.
     */
    public Builder addArrow(ImmutableBitSet lhs, ImmutableBitSet rhs) {
      arrowSet.add(Arrow.of(lhs, rhs));
      return this;
    }

    /**
     * Add an Arrow from a single determinant to a single dependent.
     */
    public Builder addArrow(int lhs, int rhs) {
      arrowSet.add(Arrow.of(lhs, rhs));
      return this;
    }

    public Builder addBidirectionalArrow(int lhs, int rhs) {
      addArrow(lhs, rhs);
      addArrow(rhs, lhs);
      return this;
    }

    public Builder addBidirectionalArrow(ImmutableBitSet lhs, ImmutableBitSet rhs) {
      addArrow(lhs, rhs);
      addArrow(rhs, lhs);
      return this;
    }

    public Builder addArrowSet(ArrowSet set) {
      for (Arrow arrow : set.getArrows()) {
        addArrow(arrow.getDeterminants(), arrow.getDependents());
      }
      return this;
    }

    /**
     * Build the ArrowSet instance and compute the functional dependency graph.
     */
    public ArrowSet build() {
      return new ArrowSet(arrowSet);
    }
  }
}
