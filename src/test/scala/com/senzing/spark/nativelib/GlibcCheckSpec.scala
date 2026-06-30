package com.senzing.spark.nativelib

import org.scalatest.funsuite.AnyFunSuite

final class GlibcCheckSpec extends AnyFunSuite {

  test("parse extracts MAJOR.MINOR") {
    assert(GlibcCheck.parse("2.34").contains((2, 34)))
    assert(GlibcCheck.parse("ldd (Ubuntu GLIBC 2.39-0ubuntu8.3) 2.39").contains((2, 39)))
    assert(GlibcCheck.parse("no version here").isEmpty)
  }

  test("check passes at or above the floor") {
    assert(GlibcCheck.check((2, 34)).isRight)
    assert(GlibcCheck.check((2, 35)).isRight)
    assert(GlibcCheck.check((3, 0)).isRight)
  }

  test("check fails below the floor") {
    assert(GlibcCheck.check((2, 33)).isLeft)
    assert(GlibcCheck.check((1, 40)).isLeft)
  }

  test("enforce throws loudly below floor or when undetectable") {
    assertThrows[RuntimeException](GlibcCheck.enforce(() => "1.20"))
    assertThrows[RuntimeException](GlibcCheck.enforce(() => "garbage"))
    GlibcCheck.enforce(() => "2.40") // no throw
  }
}
