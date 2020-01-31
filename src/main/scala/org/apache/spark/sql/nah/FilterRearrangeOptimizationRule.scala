package org.apache.spark.sql.nah

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable.ArrayBuffer

object FilterRearrangeOptimizationRule
    extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) => {
      val filters = splitConjunctivePredicates(condition)
      val updatedFilters = ArrayBuffer.empty[Expression]
      var updated = false

      // iterate over available filters
      for (filter <- filters) {
        filter match {
          case GreaterThan(a, b) => {
            if (b.isInstanceOf[AttributeReference]
                && a.isInstanceOf[Literal]) {
              updatedFilters += LessThan(b, a)
              updated = true
            } else {
              updatedFilters += filter
            }
          }
          case GreaterThanOrEqual(a, b) => {
            if (b.isInstanceOf[AttributeReference]
                && a.isInstanceOf[Literal]) {
              updatedFilters += LessThanOrEqual(b, a)
              updated = true
            } else {
              updatedFilters += filter
            }
          }
          case LessThan(a, b) => {
            if (b.isInstanceOf[AttributeReference]
                && a.isInstanceOf[Literal]) {
              updatedFilters += GreaterThan(b, a)
              updated = true
            } else {
              updatedFilters += filter
            }
          }
          case LessThanOrEqual(a, b) => {
            if (b.isInstanceOf[AttributeReference]
                && a.isInstanceOf[Literal]) {
              updatedFilters += GreaterThanOrEqual(b, a)
              updated = true
            } else {
              updatedFilters += filter
            }
          }
          case _ => updatedFilters += filter
        }
      }

      if (updated) {
        Filter(updatedFilters.reduceLeft(And), child)
      } else {
        filter
      }
    }
  }
}
