package edu.neu.coe.csye7200.csv

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers



class Spark_Calculation_Test extends AnyFlatSpec with Matchers {
  val cal = Spark_Calculation()
  behavior of "Spark_Calculation"
  it should "get the correct mean" in {
    cal.mean shouldBe 6.453200745804848
  }
}

