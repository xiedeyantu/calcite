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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Represents {@code SELECT * EXCLUDE(...)} when parsed by the Babel parser.
 */
public class SqlStarExclude extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SELECT_STAR_EXCLUDE", SqlKind.OTHER);

  private final SqlNodeList excludeList;

  public SqlStarExclude(SqlParserPos pos, SqlNodeList excludeList) {
    super(pos);
    this.excludeList = requireNonNull(excludeList, "excludeList");
  }

  public SqlNodeList getExcludeList() {
    return excludeList;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public SqlKind getKind() {
    return OPERATOR.getKind();
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(excludeList);
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlStarExclude(pos, excludeList.clone(pos));
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print("*");
    writer.print(" ");
    writer.keyword("EXCLUDE");
    writer.print("(");
    for (int i = 0; i < excludeList.size(); i++) {
      if (i > 0) {
        writer.print(", ");
      }
      excludeList.get(i).unparse(writer, 0, 0);
    }
    writer.print(")");
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    // Babel already validates the exclude list; selection expansion handles semantics.
  }
}
