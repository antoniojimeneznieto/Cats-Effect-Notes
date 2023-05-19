package cheatsheet

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.propspec.AnyPropSpec

class Calculator {
  def add(a: Int, b: Int): Int = a + b
  def subtract(a: Int, b: Int): Int = a - b
  def multiply(a: Int, b: Int): Int = a * b
  def divide(a: Int, b: Int): Int = a / b
}


class CheatsheetSuite extends AnyFunSuite {

  val calculator = new Calculator

  test("multiplication with 0 should always give 0") {
    assert(calculator.multiply(572389, 0) == 0)
    assert(calculator.multiply(-572389, 0) == 0)
    assert(calculator.multiply(0, 0) == 0)
  }

  test("dividing by 0 should throw a math error") {
    assertThrows[ArithmeticException](calculator.divide(57238, 0))
  }
}

class CalculatorSpec extends AnyFunSpec {
  val calculator = new Calculator

  // can nest as many levels deep as you like
  describe("multiplication") {
    it("should give back 0 if multiplying by 0") {
      assert(calculator.multiply(572389, 0) == 0)
      assert(calculator.multiply(-572389, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }
  }

  describe("division") {
    it("should throw a math error if dividing by 0") {
      assertThrows[ArithmeticException](calculator.divide(57238, 0))
    }
  }
}

class CalculatorWordSpec extends AnyWordSpec {
  val calculator = new Calculator

  "A calculator" should {
    "give back 0 if multiplying by 0" in {
      assert(calculator.multiply(653278, 0) == 0)
      assert(calculator.multiply(-653278, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }

    "throw a math error if dividing by 0" in {
      assertThrows[ArithmeticException](calculator.divide(653278, 0))
    }
  }

  "Two calculators" should {
    "give back 0 if multiplying by 0" in {
      assert(calculator.multiply(653278, 0) == 0)
      assert(calculator.multiply(-653278, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }
  }
}

class CalculatorFreeSpec extends AnyFreeSpec {
  val calculator = new Calculator

  "A calculator" - { // anything you want
    "give back 0 if multiplying by 0" in {
      assert(calculator.multiply(653278, 0) == 0)
      assert(calculator.multiply(-653278, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }

    "throw a math error if dividing by 0" in {
      assertThrows[ArithmeticException](calculator.divide(653278, 0))
    }
  }
}

class CalculatorPropSpec extends AnyPropSpec {
  val calculator = new Calculator

  val multiplyByZeroExamples = List((653278, 0), (-653278, 0), (0, 0))

  property("Calculator multiply by 0 should be 0") {
    assert(multiplyByZeroExamples.forall{
      case (a, b) => calculator.multiply(a, b) == 0
    })
  }

  property("Calculator divide by 0 should throw some math error") {
    assertThrows[ArithmeticException](calculator.divide(653278, 0))
  }
}
