package org.apache.spark.sql.nah

import com.bushpath.nah.spark.sql.util.Converter
import org.apache.spark.sql.nah.expressions.{BooleanExpression, BuildExpression, Contains, Covers, Equals, EqualsTollerance, Within}

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.{ArrayBuffer, Map}

object FilterInjectionOptimizationRule
    extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) => {
      val updatedBoundaries = Map[String, (Double, Double)]()
      val existingBoundaries = Map[String, (Double, Double)]()
      val attributes = Map[String, AttributeReference]()

      // iterate over available filters
      val filters = splitConjunctivePredicates(condition)
      for (filter <- filters) {
        // check if filter is a BooleanExpression
        if (filter.isInstanceOf[BooleanExpression]) {
          // retreive BooleanExpression children
          val booleanExpression = filter.asInstanceOf[BooleanExpression]
          val expressions = booleanExpression.children

          // collect AttributeReferences from BooleanExpression children
          val a = getAttributeReferences(expressions(0))
          val b = getAttributeReferences(expressions(1))
          
          // add AttributeReferences to map for later retrieval
          for (attribute <- a) {
            if (!attributes.contains(attribute.name)) {
              attributes(attribute.name) = attribute
            }
          }

          for (attribute <- b) {
            if (!attributes.contains(attribute.name)) {
              attributes(attribute.name) = attribute
            }
          }

          // process filter
          filter match {
            case _: Contains => {
              if (a.size == 0 && b.size >= 2) {
                // building bounded geometry from coordinates
                val spatialBoundaries =
                  getSpatialBoundaries(expressions(2))

                updateLowerBound(updatedBoundaries,
                  b(0).name, spatialBoundaries._1)
                updateUpperBound(updatedBoundaries,
                  b(0).name, spatialBoundaries._2)
                updateLowerBound(updatedBoundaries,
                  b(1).name, spatialBoundaries._3)
                updateUpperBound(updatedBoundaries,
                  b(1).name, spatialBoundaries._4)
              } else {
                println("unsupported injection filter: contains("
                  + a.size + "," + b.size + ")")
              }
            }
            case _: Covers => {
              if (a.size == 0 && b.size >= 2) {
                // building bounded geometry from coordinates
                val spatialBoundaries =
                  getSpatialBoundaries(expressions(2))

                updateLowerBound(updatedBoundaries,
                  b(0).name, spatialBoundaries._1)
                updateUpperBound(updatedBoundaries,
                  b(0).name, spatialBoundaries._2)
                updateLowerBound(updatedBoundaries,
                  b(1).name, spatialBoundaries._3)
                updateUpperBound(updatedBoundaries,
                  b(1).name, spatialBoundaries._4)
              } else {
                println("unsupported injection filter: covers("
                  + a.size + "," + b.size + ")")
              }
            }
            case _: Equals => {
              if (a.size >= 2 && b.size == 0) {
                // building bounded geometry from coordinates
                val spatialBoundaries =
                  getSpatialBoundaries(expressions(1))

                updateLowerBound(updatedBoundaries,
                  a(0).name, spatialBoundaries._1)
                updateUpperBound(updatedBoundaries,
                  a(0).name, spatialBoundaries._2)
                updateLowerBound(updatedBoundaries,
                  a(1).name, spatialBoundaries._3)
                updateUpperBound(updatedBoundaries,
                  a(1).name, spatialBoundaries._4)
              } else if (a.size == 0 && b.size >= 2) {
                // building bounded geometry from coordinates
                val spatialBoundaries =
                  getSpatialBoundaries(expressions(2))

                updateLowerBound(updatedBoundaries,
                  b(0).name, spatialBoundaries._1)
                updateUpperBound(updatedBoundaries,
                  b(0).name, spatialBoundaries._2)
                updateLowerBound(updatedBoundaries,
                  b(1).name, spatialBoundaries._3)
                updateUpperBound(updatedBoundaries,
                  b(1).name, spatialBoundaries._4)
              } else {
                println("unsupported injection filter: equals("
                  + a.size + "," + b.size + ")")
              }
            }
            case _: EqualsTollerance => {
              // TODO - implement injection filter
              println("TODO - support injection filter: equalsTollerance("
                + a.size + "," + b.size + ")")
            }
            case _: Within => {
              if (a.size >= 2 && b.size == 0) {
                // building bounded geometry from coordinates
                val spatialBoundaries =
                  getSpatialBoundaries(expressions(1))

                updateLowerBound(updatedBoundaries,
                  a(0).name, spatialBoundaries._1)
                updateUpperBound(updatedBoundaries,
                  a(0).name, spatialBoundaries._2)
                updateLowerBound(updatedBoundaries,
                  a(1).name, spatialBoundaries._3)
                updateUpperBound(updatedBoundaries,
                  a(1).name, spatialBoundaries._4)
              } else {
                println("unsupported injection filter: within("
                  + a.size + "," + b.size + ")")
              }
            }
            case x => println("TODO - handle filter injection for " + x.getClass)
          }
        }
 
        // check for existing boundaries
        filter match {
          case greaterThan @ GreaterThan(a, b) => {
            if (a.isInstanceOf[AttributeReference]
                && b.isInstanceOf[Literal]) {
              updateLowerBound(existingBoundaries, 
                a.asInstanceOf[AttributeReference].name,
                Converter.toDouble(b.asInstanceOf[Literal].value))
            }
          }
          case greaterThanOrEqual @ GreaterThanOrEqual(a, b) => {
            if (a.isInstanceOf[AttributeReference]
                && b.isInstanceOf[Literal]) {
              updateLowerBound(existingBoundaries, 
                a.asInstanceOf[AttributeReference].name,
                Converter.toDouble(b.asInstanceOf[Literal].value))
            }
          }
          case lessThan @ LessThan(a, b) => {
            if (a.isInstanceOf[AttributeReference]
                && b.isInstanceOf[Literal]) {
              updateUpperBound(existingBoundaries, 
                a.asInstanceOf[AttributeReference].name,
                Converter.toDouble(b.asInstanceOf[Literal].value))
            }
          }
          case lessThanOrEqual @ LessThanOrEqual(a, b) => {
            if (a.isInstanceOf[AttributeReference]
                && b.isInstanceOf[Literal]) {
              updateUpperBound(existingBoundaries, 
                a.asInstanceOf[AttributeReference].name,
                Converter.toDouble(b.asInstanceOf[Literal].value))
            }
          }
          case _ => {}
        }
      }

      // compute injected expressions
      val injectedExpressions = ArrayBuffer.empty[Expression]
      for ((name, bounds) <- updatedBoundaries) {
        // check lower bounds
        if (!existingBoundaries.contains(name)
            || existingBoundaries(name)._1 > bounds._1) {
          injectedExpressions += new GreaterThanOrEqual(
            attributes(name), new Literal(bounds._1, DoubleType))
        }

        // check upper bounds
        if (!existingBoundaries.contains(name)
            || existingBoundaries(name)._2 < bounds._2) {
          injectedExpressions += new LessThanOrEqual(
            attributes(name), new Literal(bounds._2, DoubleType))
        }
      }

      // return with injected filters
      if (injectedExpressions.size != 0) {
        injectedExpressions ++= filters
        Filter(injectedExpressions.reduceLeft(And), child)
      } else {
        filter
      }
    }
  }

  def getAttributeReferences(expression: Expression)
      : ArrayBuffer[AttributeReference] = {
    val references = ArrayBuffer.empty[AttributeReference]

    // check if expression is case class of BuildExpression
    if (expression.isInstanceOf[BuildExpression]) {
      val buildExpression = expression.asInstanceOf[BuildExpression]

      // collect AttributeReferences from buildExpression
      for (expression <- buildExpression.children) {
        if (expression.isInstanceOf[AttributeReference]) {
          references += expression.asInstanceOf[AttributeReference]
        }
      }
    } else {
      println("TODO - handle operations over BuildExpressions during injection filters")
    }

    references
  }

  def getSpatialBoundaries(expression: Expression)
      : (Double, Double, Double, Double) = {
    var minX = 180.0
    var maxX = -180.0
    var minY = 90.0
    var maxY = -90.0

    // check if expression is case class of BuildExpression
    if (expression.isInstanceOf[BuildExpression]) {
      val buildExpression = expression.asInstanceOf[BuildExpression]

      // collect AttributeReferences from buildExpression
      for ((expression, i) <- buildExpression.children.view.zipWithIndex) {
        expression match {
          case literal @ Literal(value, dataType) => {
            val double = Converter.toDouble(value)  

            if (i % 2 == 0) {
              minX = scala.math.min(minX, double)
              maxX = scala.math.max(maxX, double)
            } else {
              minY = scala.math.min(minY, double)
              maxY = scala.math.max(maxY, double)
            }
          }
        }
      }
    } else {
      println("TODO - handle operations over BuildExpressions during injection filters")
    }

    (minX, maxX, minY, maxY)
  }

  def updateLowerBound(map: Map[String, (Double, Double)],
      name: String, value: Double) = {
    if (!map.contains(name)) {
      // if bounds do not exist -> insert new bounds
      map(name) = (value, java.lang.Double.MIN_VALUE)
    } else if (value < map(name)._1) {
      // if bounds are more restrictive -> update existing lower bound
      val bounds = map(name)
      map(name) = (value, bounds._2)
    }
  }

  def updateUpperBound(map: Map[String, (Double, Double)],
      name: String, value: Double) = {
    if (!map.contains(name)) {
      // if bounds do not exist -> insert new bounds
      map(name) = (java.lang.Double.MAX_VALUE, value)
    } else if (value > map(name)._2) {
      // if bounds are more restrictive -> update existing lower bound
      val bounds = map(name)
      map(name) = (bounds._1, value)
    }
  }
}
