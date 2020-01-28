package org.apache.spark.sql.nah

import com.bushpath.nah.spark.sql.util.Converter
import org.apache.spark.sql.nah.expressions._

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, GreaterThanOrEqual, LessThanOrEqual, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ArrayBuffer

object NahFilterInjectionOptimizationRule
    extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) => {
      // iterate over available filters
      val filters = splitConjunctivePredicates(condition)
      val injectExpressions = ArrayBuffer.empty[Expression]

      for (filter <- filters) {
        // check if filter is a BooleanExpression
        if (filter.isInstanceOf[BooleanExpression]) {
          // retreive BooleanExpression children
          var booleanExpression = filter.asInstanceOf[BooleanExpression]
          var expressions = booleanExpression.children

          // collect AttributeReferences from BooleanExpression children
          var a = getAttributeReferences(expressions(0))
          var b = getAttributeReferences(expressions(1))

          // process filter
          filter match {
            case _: Within => {
              if (a.size >= 2 && b.size == 0) {
                // building bounded geometry from coordinates
                val spatialBounds = getSpatialBounds(expressions(1))

                //println("inject: " + a(0) + ">=" + spatialBounds._1)
                //println("inject: " + a(0) + "<=" + spatialBounds._2)
                //println("inject: " + a(1) + ">=" + spatialBounds._3)
                //println("inject: " + a(1) + "<=" + spatialBounds._4)

                // TODO - use Literal(spatialBounds._1, Double)
                /*val injectExpression = Seq(
					new GreaterThanOrEqual(a(0), new Literal(spatialBounds._1, DoubleType)),
					new LessThanOrEqual(a(0), new Literal(spatialBounds._2, DoubleType)),
                    new GreaterThanOrEqual(a(1), new Literal(spatialBounds._3, DoubleType)),
                    new LessThanOrEqual(a(1), new Literal(spatialBounds._4, DoubleType))
				).reduceLeft(And)*/

				injectExpressions += new GreaterThanOrEqual(a(0), new Literal(spatialBounds._1, DoubleType))
			    injectExpressions += new LessThanOrEqual(a(0), new Literal(spatialBounds._2, DoubleType))
                injectExpressions += new GreaterThanOrEqual(a(1), new Literal(spatialBounds._3, DoubleType))
                injectExpressions += new LessThanOrEqual(a(1), new Literal(spatialBounds._4, DoubleType))

              } else {
                println("unsupported injection filter: within("
                  + a.size + "," + b.size + ")")
              }
            }
            case x => println("TODO - handle filter injection for " + x.getClass)
          }
        } else {
          println("TODO - handle filter: " + filter.getClass)
          println(filter)
        }
      }

      // TODO - return with injected filters
      // TODO RETURN WITH INJECTED FILTERS
      //Filter(stayUp.reduceLeft(And), newChild)
      if (injectExpressions.size != 0) {
        injectExpressions ++= filters
        Filter(injectExpressions.reduceLeft(And), child)
      } else {
        filter
      }
    }
    case x => {
      println("TODO - handle expression : " + x.getClass)
      println(x)
      x
    }
  }

  def getAttributeReferences(expression: Expression)
      : ArrayBuffer[AttributeReference] = {
    val references = ArrayBuffer.empty[AttributeReference]

    // check if expression is case class of BuildExpression
    if (expression.isInstanceOf[BuildExpression]) {
      var buildExpression = expression.asInstanceOf[BuildExpression]

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

  def getSpatialBounds(expression: Expression)
      : (Double, Double, Double, Double) = {
    var minX = 180.0
    var maxX = -180.0
    var minY = 90.0
    var maxY = -90.0

    // check if expression is case class of BuildExpression
    if (expression.isInstanceOf[BuildExpression]) {
      var buildExpression = expression.asInstanceOf[BuildExpression]

      // collect AttributeReferences from buildExpression
      for (expression <- buildExpression.children) {
        expression match {
          case literal @ Literal(value, dataType) => {
            var double = Converter.toDouble(value)  

            // TODO - only compute on every other
            minX = scala.math.min(minX, double)
            maxX = scala.math.max(maxX, double)
            minY = scala.math.min(minY, double)
            maxY = scala.math.max(maxY, double)
          }
        }
      }
    } else {
      println("TODO - handle operations over BuildExpressions during injection filters")
    }

    (minX, maxX, minY, maxY)
  }
}
