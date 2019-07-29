package org.apache.spark.sql.atlas

import org.apache.spark.sql.atlas.expressions._

import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.v2.DataSourceV2

object AtlasPredicatePushdownOptimizationRule
    extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, unaryNode: UnaryNode) => {
      val (pushDown, stayUp) = splitConjunctivePredicates(condition)
        .partition(this.isBoundingGeometryExpression(_))

      //println("TODO - pushdown predicate:" + condition
      //  + " through child:" + child)
      println("PUSHDOWN: " + pushDown)
      println("STAYUP: " + stayUp)

      if (pushDown.nonEmpty) {
        //val newChild = insertFilter(pushDown.reduceLeft(And))
        val newChild = unaryNode.withNewChildren(
          Seq(Filter(pushDown.reduceLeft(And), unaryNode.child)))
        if (stayUp.nonEmpty) {
          Filter(stayUp.reduceLeft(And), newChild)
        } else {
          newChild
        }
      } else {
        filter
      }
    }
    case filter @ Filter(condition, child) => {
      //println("TODO - handle '" + condition + "' : '" + child + "'")
      //println("DETERMINISTIC:" + condition.deterministic)
      filter
    }
  }

  def isBoundingGeometryExpression(expression: Expression): Boolean = {
    expression match {
      // TODO
      case _: Within => true
      case _ => false
    }
  }
}
