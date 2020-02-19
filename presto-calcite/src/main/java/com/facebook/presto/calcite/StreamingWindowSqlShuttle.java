package com.facebook.presto.calcite;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class StreamingWindowSqlShuttle extends SqlShuttle {
  private SqlNode timeField;
  private SqlNode interval;

  public StreamingWindowSqlShuttle(SqlNode timeField, SqlNode interval) {
    this.timeField = timeField;
    this.interval = interval;
  }
  @Override
  public SqlNode visit(SqlCall call) {
    for (int i = 0;i < call.getOperandList().size(); ++i) {
      SqlNode original = call.getOperandList().get(i);
      if (original != null) {
        SqlNode modifiedNode = original.accept(this);
        if (original != modifiedNode) {
          call.setOperand(i, modifiedNode);
        }
      }
    }
    return call;
  }

  @Override
  public SqlNode visit(SqlIdentifier identifier) {
    if (identifier.isSimple() && identifier.getSimple().equalsIgnoreCase("wend")) {
      return new SqlBasicCall(SqlStdOperatorTable.TUMBLE_END,
          new SqlNode[]{timeField, interval}, SqlParserPos.ZERO);
    } else if (identifier.isSimple() && identifier.getSimple().equalsIgnoreCase("wstart")) {
      return new SqlBasicCall(SqlStdOperatorTable.TUMBLE_START,
          new SqlNode[]{timeField, interval}, SqlParserPos.ZERO);
    } else {
      return identifier;
    }
  }

  private SqlNode deepCloneSqlCall(SqlCall call) {
    ArgHandler<SqlNode> argHandler = new SqlShuttle.CallCopyingArgHandler(call, false);
    call.getOperator().acceptCall(this, call, false, argHandler);
    return argHandler.result();
  }
}
