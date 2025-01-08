package org.grapheco.lynx.dataframe

import org.grapheco.lynx.evaluator.{ExpressionContext, ExpressionEvaluator}
import org.grapheco.lynx.types.{LynxType, LynxValue}
import org.opencypher.v9_0.expressions.Expression

class DefaultDataFrameOperator(expressionEvaluator: ExpressionEvaluator) extends DataFrameOperator {

  // 添加缓存机制以避免重复计算相同的表达式
  private val evalCache = scala.collection.mutable.Map[(Expression, Map[String, LynxValue]), LynxValue]()

  /**
   * 缓存表达式的计算结果，避免重复计算。
   */
  private def cachedEval(expression: Expression, ctx: ExpressionContext): LynxValue = {
    val key = (expression, ctx.variables)
    evalCache.getOrElseUpdate(key, expressionEvaluator.eval(expression)(ctx))
  }

  /**
   * 提取创建上下文的公共方法，减少重复代码。
   */
  private def createContext(record: Seq[LynxValue], ctx: ExpressionContext, columnsName: Seq[String]): ExpressionContext =
    ctx.withVars(columnsName.zip(record).toMap)

  override def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame = {
    val sourceSchema = df.schema.toMap
    val columnNameIndex = df.columnsName.zipWithIndex.toMap
    val newSchema = columns.map { case (colName, alias) => alias.getOrElse(colName) -> sourceSchema(colName) }
    val usedIndex = columns.map(_._1).map(columnNameIndex)

    DataFrame(newSchema, () => df.records.map(row => usedIndex.map(row.apply)))
  }

  override def filter(df: DataFrame, predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame =
    DataFrame(df.schema, () => df.records.filter(predicate))

  override def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val newSchema = columns.map { case (name, expr) => name -> expressionEvaluator.typeOf(expr, df.schema.toMap) }

    DataFrame(newSchema, () => df.records.map { record =>
      val recordCtx = createContext(record, ctx, df.columnsName) // 使用提取的公共方法创建上下文
      columns.map { case (_, expr) => cachedEval(expr, recordCtx) } // 使用缓存机制避免重复计算
    })
  }

  override def groupBy(df: DataFrame, groupings: Seq[(String, Expression)], aggregations: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = {
    val newSchema = (groupings ++ aggregations).map { case (name, expr) => name -> expressionEvaluator.typeOf(expr, df.schema.toMap) }
    val columnsName = df.columnsName

    DataFrame(newSchema, () => {
      if (groupings.nonEmpty) {
        df.records.iterator.map { record =>
          val recordCtx = createContext(record, ctx, columnsName) // 使用提取的公共方法创建上下文
          val groupingValues = groupings.map { case (_, expr) => cachedEval(expr, recordCtx) } // 使用缓存机制避免重复计算
          groupingValues -> recordCtx
        }.toSeq.groupBy(_._1).iterator.flatMap { case (groupingValue, recordsCtx) =>
          Iterator(groupingValue ++ aggregations.map { case (_, expr) => expressionEvaluator.aggregateEval(expr)(recordsCtx.map(_._2)) })
        }
      } else {
        val allRecordsCtx = df.records.iterator.map { record => createContext(record, ctx, columnsName) }.toSeq // 使用提取的公共方法创建上下文
        Iterator(aggregations.map { case (_, expr) => expressionEvaluator.aggregateEval(expr)(allRecordsCtx) })
      }
    })
  }

  override def skip(df: DataFrame, num: Int): DataFrame =
    DataFrame(df.schema, () => df.records.drop(num))

  override def take(df: DataFrame, num: Int): DataFrame =
    DataFrame(df.schema, () => df.records.take(num))

  /**
   * 增加异常处理和参数校验，确保输入参数的有效性。
   */
  override def join(a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType): DataFrame = {
    if (a == null || b == null || joinColumns.isEmpty) {
      throw new IllegalArgumentException("Invalid input parameters for join operation.")
    }
    SortMergeJoiner.join(a, b, joinColumns, joinType)
  }

  override def cross(a: DataFrame, b: DataFrame): DataFrame =
    DataFrame(a.schema ++ b.schema, () => a.records.flatMap(ra => b.records.map(ra ++ _)))

  /**
   * 使用迭代器代替数组，减少中间数据结构的创建。
   */
  override def distinct(df: DataFrame): DataFrame =
    DataFrame(df.schema, () => df.records.toSeq.distinct.iterator)

  /**
   * 优化 `orderBy` 方法，使用迭代器和缓存机制提高性能。
   */
  override def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame = {
    val columnsName = df.columnsName
    DataFrame(df.schema, () => df.records.toArray.sortWith { (A, B) =>
      val ctxA = createContext(A, ctx, columnsName) // 使用提取的公共方法创建上下文
      val ctxB = createContext(B, ctx, columnsName) // 使用提取的公共方法创建上下文
      val sortValue = sortItem.map {
        case (exp, asc) =>
          (cachedEval(exp, ctxA), cachedEval(exp, ctxB), asc) // 使用缓存机制避免重复计算
      }
      _ascCmp(sortValue.iterator)
    }.iterator)
  }

  /**
   * 简化排序比较逻辑，使用 `collectFirst` 替代显式的 `while` 循环。
   */
  private def _ascCmp(sortValue: Iterator[(LynxValue, LynxValue, Boolean)]): Boolean = {
    sortValue.collectFirst {
      case (a, b, asc) if a.compareTo(b) != 0 => a.compareTo(b) > 0 != asc
    }.getOrElse(false)
  }
}package org.grapheco.lynx.dataframe

