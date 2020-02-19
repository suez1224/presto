/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.calcite;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Node;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Reader;

public class CalciteAstTest {

  private static final SqlParser PRESTO_PARSER = new SqlParser();
  private static org.apache.calcite.sql.parser.SqlParser.Config sqlParserConfig =
      org.apache.calcite.sql.parser.SqlParser.configBuilder()
          .setCaseSensitive(false)
          .setUnquotedCasing(Casing.UNCHANGED)
          .setQuotedCasing(Casing.UNCHANGED)
          .setConformance(SqlConformanceEnum.ORACLE_10)
          .build();

  protected org.apache.calcite.sql.parser.SqlParser getCalciteSqlParser(String sql) {
    return getCalciteSqlParser(new SourceStringReader(sql));
  }

  protected org.apache.calcite.sql.parser.SqlParser getCalciteSqlParser(Reader source) {
    return org.apache.calcite.sql.parser.SqlParser.create(source, sqlParserConfig);
  }

  @Test
  public void testAlias() {
    Node prestoNode = PRESTO_PARSER.createStatement("SELECT msg.field1.UUID as uuid from table1");
    SqlNode translatedCalciteNode = new CalciteAstTranslator().process(prestoNode);

    Assert.assertEquals(translatedCalciteNode.toString(),
        "SELECT `msg`.`field1`.`UUID` AS `uuid`\n" +
            "FROM `table1`");
  }

  @Test
  public void testCast() {
    Node prestoNode = PRESTO_PARSER.createStatement("SELECT CAST(msg.field1.int1 as DOUBLE) as double1 from table1");
    SqlNode translatedCalciteNode = new CalciteAstTranslator().process(prestoNode);

    Assert.assertEquals(translatedCalciteNode.toString(),
        "SELECT CAST(`msg`.`field1`.`int1` AS DOUBLE) AS `double1`\n" +
            "FROM `table1`");
  }

  @Test
  public void testFunctionCall() {
    Node prestoNode = PRESTO_PARSER.createStatement("SELECT add(msg.field1.int1, msg.field1.int2) as sum from table1");
    SqlNode translatedCalciteNode = new CalciteAstTranslator().process(prestoNode);

    Assert.assertEquals(translatedCalciteNode.toString(),
        "SELECT `add`(`msg`.`field1`.`int1`, `msg`.`field1`.`int2`) AS `sum`\n" +
            "FROM `table1`");
  }

  @Test
  public void testFunctionCallWithCast() {
    Node prestoNode = PRESTO_PARSER.createStatement("SELECT cast(add(msg.field1.int1, msg.field1.int2) AS DOUBLE) as v1 from table1");
    SqlNode translatedCalciteNode = new CalciteAstTranslator().process(prestoNode);

    Assert.assertEquals(translatedCalciteNode.toString(),
        "SELECT CAST(`add`(`msg`.`field1`.`int1`, `msg`.`field1`.`int2`) AS DOUBLE) AS `v1`\n" +
            "FROM `table1`");
  }

  @Test
  public void testCaseExpr() {
    Node prestoNode = PRESTO_PARSER.createStatement("SELECT (CASE WHEN msg.field1.obj1 IS NULL THEN 0 ELSE 1 END) as v1 from table1");
    SqlNode translatedCalciteNode = new CalciteAstTranslator().process(prestoNode);

    Assert.assertEquals(translatedCalciteNode.toString(),
        "SELECT CASE WHEN `msg`.`field1`.`obj1` IS NULL THEN 0 ELSE 1 END AS `v1`\n" +
            "FROM `table1`");
  }

  @Test
  public void testSelectSubQuery() {
    Node prestoNode = PRESTO_PARSER.createStatement("SELECT a, b, c from (select a, b, c from table1)");
    SqlNode translatedCalciteNode = new CalciteAstTranslator().process(prestoNode);

    Assert.assertEquals(translatedCalciteNode.toString(),
        "SELECT `a`, `b`, `c`\n" + "" +
            "FROM (SELECT `a`, `b`, `c`\n"
            + "FROM `table1`)");
  }


  @Test
  public void testSimpleGroupBy() throws SqlParseException {
    final String query = "SELECT msg.field1.UUID AS uuid,\n" +
        "CAST(SUM(msg.work_time/1000) AS DOUBLE) AS sum_30min,\n" +
        "CAST(SUM(CASE WHEN msg.work_time IS NULL THEN 0 ELSE 1 END) AS INT) AS count_30min,\n" +
        "CAST(unix_time(wend)/1000 AS BIGINT) AS ts_bucket,\n" +
        "MAX(wstart) as windowStart,\n" +
        "wend as windowEnd\n" +
        "FROM TABLE(TUMBLE(hdrone.table1, ts, INTERVAL '30' MINUTE))\n" +
        "GROUP BY wend, msg.field1.UUID";
    Node prestoNode = PRESTO_PARSER.createStatement(query);
    SqlNode calciteNode = getCalciteSqlParser(query).parseStmt();
    SqlNode translatedCalciteNode = new CalciteAstTranslator().process(prestoNode);

    Assert.assertEquals(translatedCalciteNode.toString(),
        "SELECT `msg`.`field1`.`UUID` AS `uuid`, " +
            "CAST(SUM(`msg`.`work_time` / 1000) AS DOUBLE) AS `sum_30min`, " +
            "CAST(SUM(CASE WHEN `msg`.`work_time` IS NULL THEN 0 ELSE 1 END) AS INTEGER) AS `count_30min`, " +
            "CAST(`unix_time`(TUMBLE_END(`ts`, INTERVAL '30' MINUTE)) / 1000 AS BIGINT) AS `ts_bucket`, " +
            "MAX(TUMBLE_START(`ts`, INTERVAL '30' MINUTE)) AS `windowStart`, " +
            "TUMBLE_END(`ts`, INTERVAL '30' MINUTE) AS `windowEnd`\n" +
            "FROM `hdrone`.`table1`\n" +
            "GROUP BY `msg.field1.UUID`, TUMBLE(`ts`, INTERVAL '30' MINUTE)");
  }
}
