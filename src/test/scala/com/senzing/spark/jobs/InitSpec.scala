package com.senzing.spark.jobs

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite

final class InitSpec extends AnyFunSuite {

  test("ddlFileName branches by dialect; SQLite has none; unknown throws") {
    assert(SchemaApplier.ddlFileName("postgresql").contains("szcore-schema-postgresql-create.sql"))
    assert(SchemaApplier.ddlFileName("mysql").contains("szcore-schema-mysql-create.sql"))
    assert(SchemaApplier.ddlFileName("mssql").contains("szcore-schema-mssql-create.sql"))
    assert(SchemaApplier.ddlFileName("sqlite").isEmpty)
    assertThrows[IllegalArgumentException](SchemaApplier.ddlFileName("oracle"))
  }

  test("ensureSchema applies DDL only when absent") {
    val applied = new AtomicInteger(0)
    assert(SchemaApplier.ensureSchema(() => true, () => applied.incrementAndGet()) == false)
    assert(applied.get() == 0)
    assert(SchemaApplier.ensureSchema(() => false, () => applied.incrementAndGet()) == true)
    assert(applied.get() == 1)
  }

  test("ensureDefaultConfig registers only on a greenfield repo") {
    val registers = new AtomicInteger(0)
    // greenfield: getDefault == 0 → register+set, returns the new id
    val id1 = InitJob.ensureDefaultConfig(() => 0L, () => { registers.incrementAndGet(); 42L })
    assert(id1 == 42L && registers.get() == 1)
    // existing: getDefault != 0 → no registration, returns the current id
    val id2 = InitJob.ensureDefaultConfig(() => 7L, () => { registers.incrementAndGet(); 99L })
    assert(id2 == 7L && registers.get() == 1)
  }
}
