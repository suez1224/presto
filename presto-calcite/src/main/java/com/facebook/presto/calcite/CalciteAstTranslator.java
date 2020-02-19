package com.facebook.presto.calcite;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableFunction;
import com.facebook.presto.sql.tree.TableSubquery;

import com.facebook.presto.sql.tree.WhenClause;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CalciteAstTranslator extends AstVisitor<SqlNode, Void> {

  static List<String> flattenDereferenceExpression(DereferenceExpression expression) {
    List<String> ret;
    if (expression.getBase() instanceof DereferenceExpression) {
      ret = flattenDereferenceExpression((DereferenceExpression) expression.getBase());
    } else if (expression.getBase() instanceof Identifier) {
      ret = new ArrayList<>();
      ret.add(((Identifier) expression.getBase()).getValue());
    } else {
      throw new UnsupportedOperationException("NOT supported Expression: " + expression.getBase());
    }
    ret.add(expression.getField().getValue());
    return ret;
  }

  static SqlTypeName toSqlTypeName(String prestoTypeName) {
    if (prestoTypeName.equalsIgnoreCase("DOUBLE")) {
      return SqlTypeName.DOUBLE;
    } else if (prestoTypeName.equalsIgnoreCase("int")) {
      return SqlTypeName.INTEGER;
    } else if (prestoTypeName.equalsIgnoreCase("bigint")) {
      return SqlTypeName.BIGINT;
    }
    throw new UnsupportedOperationException("Unsupported type: " + prestoTypeName);
  }

  @Override
  protected SqlNode visitDereferenceExpression(DereferenceExpression expression, Void context) {
    return new SqlIdentifier(flattenDereferenceExpression(expression),
        toSqlParserPos(expression.getLocation()));
  }

  @Override
  protected SqlNode visitCast(Cast cast, Void context) {
    return new SqlBasicCall(SqlStdOperatorTable.CAST,
        new SqlNode[]{
            process(cast.getExpression(), context),
            new SqlDataTypeSpec(
                new SqlBasicTypeNameSpec(toSqlTypeName(cast.getType()), SqlParserPos.ZERO),
                SqlParserPos.ZERO)}, toSqlParserPos(cast.getLocation()));
  }

  @Override
  protected SqlNode visitFunctionCall(FunctionCall call, Void context) {
    return new SqlBasicCall(
        new SqlUnresolvedFunction(
            new SqlIdentifier(call.getName().getParts(), toSqlParserPos(call.getLocation())),
            null,
            null,
            null,
            null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        call.getArguments().stream().map(expression1 -> process(expression1, context)).toArray(SqlNode[]::new),
        toSqlParserPos(call.getLocation()));
  }

  @Override
  protected SqlNode visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
    List<SqlNode> whenList = new ArrayList<>();
    List<SqlNode> thenList = new ArrayList<>();
    node.getWhenClauses().stream().forEach(whenClause -> {
      whenList.add(process(whenClause.getOperand(), context));
      thenList.add(process(whenClause.getResult(), context));
    });
    SqlParserPos pos = toSqlParserPos(node.getLocation());
    return new SqlCase(
        pos,
        null,
        new SqlNodeList(whenList, pos),
        new SqlNodeList(thenList, pos),
        node.getDefaultValue().isPresent() ?
            process(node.getDefaultValue().get(), context) : null);
  }

  @Override
  protected SqlNode visitExpression(Expression expression, Void context) {
    throw new UnsupportedOperationException("NOT supported Expression: " + expression);
  }

  static TimeUnit toTimeUnit(IntervalLiteral.IntervalField intervalField) {
    switch (intervalField) {
      case YEAR:
        return TimeUnit.YEAR;
      case MONTH:
        return TimeUnit.MONTH;
      case DAY:
        return TimeUnit.DAY;
      case HOUR:
        return TimeUnit.HOUR;
      case MINUTE:
        return TimeUnit.MINUTE;
      case SECOND:
        return TimeUnit.SECOND;
      default:
        throw new UnsupportedOperationException("NOT supported IntervalField: " + intervalField);
    }
  }

  @Override
  protected SqlNode visitIntervalLiteral(IntervalLiteral node, Void context) {
    return SqlLiteral.createInterval(
        node.getSign() == IntervalLiteral.Sign.POSITIVE ? 1 : -1,
        node.getValue(),
        new SqlIntervalQualifier(toTimeUnit(node.getStartField()),
            node.getEndField().isPresent() ? toTimeUnit(node.getEndField().get()) : null,
            toSqlParserPos(node.getLocation())),
        toSqlParserPos(node.getLocation()));
  }

  @Override
  protected SqlNode visitIdentifier(Identifier node, Void context) {
    return new SqlIdentifier(node.getValue(), toSqlParserPos(node.getLocation()));
  }

  @Override
  protected SqlNode visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
  {
    switch (node.getOperator()) {
      case DIVIDE:
        return new SqlBasicCall(SqlStdOperatorTable.DIVIDE,
            new SqlNode[]{
                process(node.getLeft(), context),
                process(node.getRight(), context)
            },
            toSqlParserPos(node.getLocation()));
      case ADD:
        return new SqlBasicCall(SqlStdOperatorTable.PLUS,
            new SqlNode[]{
                process(node.getLeft(), context),
                process(node.getRight(), context)
            },
            toSqlParserPos(node.getLocation()));
      case SUBTRACT:
        return new SqlBasicCall(SqlStdOperatorTable.MINUS,
            new SqlNode[]{
                process(node.getLeft(), context),
                process(node.getRight(), context)
            },
            toSqlParserPos(node.getLocation()));
      case MULTIPLY:
        return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY,
            new SqlNode[]{
                process(node.getLeft(), context),
                process(node.getRight(), context)
            },
            toSqlParserPos(node.getLocation()));
      case MODULUS:
        return new SqlBasicCall(SqlStdOperatorTable.MOD,
            new SqlNode[]{
                process(node.getLeft(), context),
                process(node.getRight(), context)
            },
            toSqlParserPos(node.getLocation()));
      default:
        throw new UnsupportedOperationException("NOT supported Operator: " + node.getOperator());
    }
  }

  @Override
  protected SqlNode visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
  {
    return new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL,
        new SqlNode[]{ process(node.getValue(), context) },
        toSqlParserPos(node.getLocation()));
  }

  @Override
  protected SqlNode visitIsNullPredicate(IsNullPredicate node, Void context)
  {
    return new SqlBasicCall(SqlStdOperatorTable.IS_NULL,
        new SqlNode[]{ process(node.getValue(), context) },
        toSqlParserPos(node.getLocation()));
  }

  @Override
  protected SqlNode visitLiteral(Literal literal, Void context) {
    if (literal instanceof LongLiteral) {
      LongLiteral ll = (LongLiteral) literal;
      return SqlLiteral.createExactNumeric(Long.toString(ll.getValue()),
          toSqlParserPos(literal.getLocation()));
    }
    throw new UnsupportedOperationException("NOT supported Literal: " + literal);
  }

  @Override
  protected SqlNode visitSelect(Select node, Void context) {
    return new SqlNodeList(
        node.getSelectItems().stream().map(selectItem -> {
          if (selectItem instanceof AllColumns) {
            return new SqlIdentifier("*", toSqlParserPos(selectItem.getLocation()));
          } else if (selectItem instanceof SingleColumn) {
            SingleColumn sc = (SingleColumn) selectItem;
            if (sc.getAlias().isPresent()) {
              Identifier alias = sc.getAlias().get();
              return new SqlBasicCall(new SqlAsOperator(),
                  new SqlNode[]{
                      process(sc.getExpression(), context),
                      new SqlIdentifier(alias.getValue(), toSqlParserPos(alias.getLocation()))}
                  , toSqlParserPos(sc.getLocation()));
            } else {
              return process(sc.getExpression(), context);
            }
          }
          throw new UnsupportedOperationException("NOT supported SelectItem: " + selectItem);
        }).collect(Collectors.toList()),
        SqlParserPos.ZERO
    );
  }

  static protected SqlParserPos toSqlParserPos(Optional<NodeLocation> nodeLocationOpt) {
    if (nodeLocationOpt.isPresent()) {
      return new SqlParserPos(nodeLocationOpt.get().getLineNumber(),
          nodeLocationOpt.get().getColumnNumber());
    } else {
      return SqlParserPos.ZERO;
    }
  }

  @Override
  protected SqlNode visitTable(Table node, Void context) {
    return new SqlIdentifier(node.getName().getParts(), toSqlParserPos(node.getLocation()));
  }

  @Override
  protected SqlNode visitTableFunction(TableFunction node, Void context) {
    if (node.getName().toString().equalsIgnoreCase("TUMBLE")) {
      return new SqlIdentifier(node.getName().getParts(), toSqlParserPos(node.getLocation()));
    } else {
      return new SqlBasicCall(SqlStdOperatorTable.COLLECTION_TABLE,
          new SqlNode[]{new SqlBasicCall(
              new SqlUnresolvedFunction(
                  new SqlIdentifier(node.getName().getParts(), toSqlParserPos(node.getLocation())),
                  null,
                  null,
                  null,
                  null,
                  SqlFunctionCategory.USER_DEFINED_FUNCTION),
              node.getExpressions().stream().map(expression1 -> process(expression1, context)).toArray(SqlNode[]::new),
              toSqlParserPos(node.getLocation()))},
          toSqlParserPos(node.getLocation()));
    }
  }

  @Override
  protected SqlNode visitGroupBy(GroupBy node, Void context) {
    return new SqlNodeList(
        node.getGroupingElements().stream().map(groupingElement -> {
          if (groupingElement instanceof SimpleGroupBy) {
            return new SqlIdentifier(groupingElement.getExpressions().get(0).toString(),
                toSqlParserPos(groupingElement.getExpressions().get(0).getLocation()));
          }
          return null;
        }).collect(Collectors.toList()), toSqlParserPos(node.getLocation())
    );
  }

  @Override
  protected SqlNode visitQuery(Query node, Void context) {
    return process(node.getQueryBody(), context);
  }

  @Override
  protected SqlNode visitTableSubquery(TableSubquery node, Void context) {
    return process(node.getQuery(), context);
  }

  static IntervalLiteral extractTumbleInterval(TableFunction tableFunction) {
    for (Expression expression : tableFunction.getExpressions()) {
      if (expression instanceof IntervalLiteral) {
        return (IntervalLiteral) expression;
      }
    }
    throw new RuntimeException("Expect interval parameter in Tumble Function!");
  }

  static Identifier extractTimeField(TableFunction tableFunction) {
    for (Expression expression : tableFunction.getExpressions()) {
      if (expression instanceof Identifier) {
        return (Identifier) expression;
      }
    }
    throw new RuntimeException("Expect interval parameter in Tumble Function!");
  }

  static SqlNodeList rewriteSelectItemsForTumble(
      SqlNodeList selectItems, SqlNode timeSqlNode, SqlNode tumbleIntervalSqlNode) {
    List<SqlNode> nodeList = selectItems.getList();
    SqlShuttle sqlShuttle = new StreamingWindowSqlShuttle(timeSqlNode, tumbleIntervalSqlNode);
    return new SqlNodeList(nodeList.stream()
        .map(sqlNode -> sqlNode.accept(sqlShuttle))
        .collect(Collectors.toList()), selectItems.getParserPosition());
  }

  @Override
  protected SqlNode visitQuerySpecification(QuerySpecification node, Void context) {
    if (node.getFrom().isPresent() && node.getFrom().get() instanceof TableFunction &&
        ((TableFunction) node.getFrom().get()).getName().toString().equalsIgnoreCase("tumble")) {
      TableFunction func = (TableFunction) node.getFrom().get();
      Identifier timeField = (Identifier) func.getExpressions().get(1);
      IntervalLiteral tumbleInterval = (IntervalLiteral) func.getExpressions().get(2);

      SqlBasicCall tumbleCall = new SqlBasicCall(
          new SqlUnresolvedFunction(
              new SqlIdentifier(func.getName().getParts(), toSqlParserPos(func.getLocation())),
              null,
              null,
              null,
              null,
              SqlFunctionCategory.USER_DEFINED_FUNCTION),
          func.getExpressions().stream().skip(1).map(expression -> process(expression, context)).toArray(SqlNode[]::new),
          toSqlParserPos(func.getLocation())
      );
      assert(node.getGroupBy().isPresent());
      SqlNodeList groupBy = ((SqlNodeList) process(node.getGroupBy().get(), context));
      groupBy.getList().removeIf(sqlNode -> sqlNode instanceof SqlIdentifier &&
              (((SqlIdentifier) sqlNode).getSimple().equalsIgnoreCase("wstart") ||
                  ((SqlIdentifier) sqlNode).getSimple().equalsIgnoreCase("wend")));
      groupBy.add(tumbleCall);
      SqlNodeList selectItems = rewriteSelectItemsForTumble(
          (SqlNodeList) process(node.getSelect(), context),
          process(timeField, context), process(tumbleInterval, context));
      return new SqlSelect(
          SqlParserPos.ZERO,
          null,
          selectItems,
          node.getFrom().isPresent() ?
              process(new Table(
                  QualifiedName.of(flattenDereferenceExpression((DereferenceExpression) func.getExpressions().get(0)))), context) : null,
          null,
          groupBy,
          null,
          null,
          null,
          null,
          null);
    } else{
      return new SqlSelect(
          SqlParserPos.ZERO,
          null,
          (SqlNodeList) process(node.getSelect(), context),
          node.getFrom().isPresent() ? process(node.getFrom().get(), context) : null,
          null,
          node.getGroupBy().isPresent() ? (SqlNodeList) process(node.getGroupBy().get(), context) : null,
          null,
          null,
          null,
          null,
          null);
    }
  }

  @Override
  public SqlNode process(Node node, @Nullable Void context) {
    return super.process(node, context);
  }
}