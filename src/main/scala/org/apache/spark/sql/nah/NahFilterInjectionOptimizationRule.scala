package org.apache.spark.sql.nah

import org.apache.spark.sql.nah.expressions._

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.v2.DataSourceV2

object NahFilterInjectionOptimizationRule
    extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) => {
      // TODO - retrieve all DataSourceV2Relations with getName == "nah"
      // currently just checking if the first child is correct
      /*if (child.isInstanceOf[DataSourceV2Relation]) {
        val relation = child.asInstanceOf[DataSourceV2Relation]
        if (relation.source.isInstanceOf[NahSource]) {
          val source = relation.source.asInstanceOf[NahSource]
            // TODO - retrieve spatial feature names
        }
      }*/

      // TODO - discover spatial fields from data sources
      println("childClass: " + child.getClass())
      println("child: " + child)

      filter
    }
  }

  def isBoundingGeometryExpression(expression: Expression): Boolean = {
    // TODO - finish adding potential bounding geometries
    expression match {
      case _: Contains => true
      case _: Covers => true
      case _: Within => true
      case _: Overlaps => true
      case _ => false
    }
  }
}
