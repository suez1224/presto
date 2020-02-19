package com.facebook.presto.sql.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class TableFunction extends QueryBody {

  private QualifiedName name;
  private List<Expression> expressions;

  public TableFunction(NodeLocation location, QualifiedName name, List<Expression> expressions)
  {
    super(Optional.of(location));
    this.name = name;
    this.expressions = expressions;
  }

  public List<Expression> getExpressions()
  {
    return expressions;
  }

  @Override
  public List<? extends Node> getChildren()
  {
    return expressions;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, expressions);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableFunction tableFunction = (TableFunction) o;
    return Objects.equals(name, tableFunction.name)
        && Objects.equals(expressions, tableFunction.expressions);
  }

  @Override
  public String toString()
  {
    return toStringHelper(this)
        .addValue(name)
        .add("expressions", expressions)
        .toString();
  }

  public QualifiedName getName() {
    return name;
  }
}
