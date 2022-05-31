import scala.reflect.runtime.universe.TypeTag

import io.delta.tables.DeltaTable
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, lit}

case class Address (street: String, city: String, state: String, zip: String)
case class Phone (number: String, label: String)
case class Person(name: String, age: Int, married: Boolean, phones: Array[Phone], address: Address, income: Double)
case class PersonWithGender(name: String, age: Int, gender: String, phones: Array[Phone], address: Address, income: Double)

//val spark = SparkSession.builder().appName("Delta Lake basic example").getOrCreate()

import spark.implicits._

val location = "/delta/export/person"
def saveEntity[T: Encoder: TypeTag](entity: T, mergeSchema: Boolean = false): Unit = {
    Seq(entity).toDS().write.partitionBy("age").format("delta").mode("append").option("mergeSchema", "true").save(location)
}
//val savePersons = (data: Seq[Person]) => data.toDS().write.partitionBy("age").format("delta").mode("append").save(location)
//val savePerson = (data: Person) => savePersons(Seq(data))

saveEntity(Person("Alice", 42, true, Array(Phone("123-555-0000", "Cell"), Phone("123-444-0001", "Home")), Address("100 Main St", "Anytown", "NY", "12345"), 110000.00))
saveEntity(Person("Andy", 30, true, Array(Phone("123-555-0001", "Cell")), Address("101 Main St", "Anytown", "NY", "12345"), 80000.00))
saveEntity(Person("Betty", 42, false, Array(Phone("123-555-0002", "Cell"), Phone("123-444-0002", "Home")), Address("102 Main St", "Anytown", "NY", "12345"), 120000.00))
saveEntity(Person("Bob", 42, false, Array(Phone("123-555-0003", "Cell"), Phone("123-444-0003", "Home")), Address("103 Main St", "Anytown", "NY", "12345"), 99000.00))
saveEntity(Person("Carrie", 25, true, Array(Phone("123-555-0004", "Cell")), Address("104 Main St", "Anytown", "NY", "12345"), 75000.00))
saveEntity(Person("Charlie", 30, true, Array(Phone("123-555-0005", "Cell")), Address("105 Main St", "Anytown", "NY", "12345"), 75000.00))
// generate some removes in the transaction log
DeltaTable.forPath(location).
        update(
            condition = expr("name like 'B%'"),
            set = Map("age" -> lit("21")))
DeltaTable.forPath(location).
        update(
            condition = expr("married = true"),
            set = Map("income" -> expr("income + 1000")))
saveEntity(Person("Dolly", 25, false, Array(Phone("123-555-0006", "Cell")), Address("106 Main St", "Anytown", "NY", "12345"), 65000.00))
saveEntity(Person("Drake", 25, true, Array(Phone("123-555-0007", "Cell")), Address("107 Main St", "Anytown", "NY", "12345"), 72000.00))
saveEntity(Person("Emma", 42, true, Array(Phone("123-555-0008", "Cell")), Address("108 Main St", "Anytown", "NY", "12345"), 101000.00))
saveEntity(Person("Earl", 30, false, Array(Phone("123-555-0009", "Cell")), Address("109 Main St", "Anytown", "NY", "12345"), 88000.00))
// change schema
saveEntity(PersonWithGender("Fay", 28, "F", Array(), Address("110 Main St", "Anytown", "NY", "12345"), 25000.00), true)
saveEntity(PersonWithGender("Frank", 29, "M", Array(Phone("123-555-0010", "Cell")), Address("111 Main St", "Anytown", "NY", "12345"), 22000.00))
