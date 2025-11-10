package Lab5

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
object lab7 {

  case class StoreInfo(storeName: String, storeCity: String, totalSales: Double)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val dataLineItem = sc.textFile("src/main/scala/lineItem_mock.csv")
    val headerLineItem = dataLineItem.first()
    val lineItem = dataLineItem.filter(line => line != headerLineItem).map(line =>
      (line.split(",")(3).trim().toInt, (line.split(",")(2).trim().toInt, line.split(",")(4).trim().toInt)))
    // productID, salesID, quantity

    val dataProduct = sc.textFile("src/main/scala/product_mock.csv")
    val headerProduct = dataProduct.first()
    val product = dataProduct.filter(line => line != headerProduct).map(line =>
      (line.split(",")(0).trim().toInt, line.split(",")(2).trim().toDouble))
    // productID, price

    val dataSales = sc.textFile("src/main/scala/sales.csv")
    val headerSales = dataSales.first()
    val sales = dataSales.filter(line => line != headerSales).map { line =>
      val parts = line.split(",")
      val salesID = parts(0).trim().toInt
      val yearMonth = parts(1).trim()
      val storeID = parts(3).trim().toInt
      (salesID, (yearMonth.substring(0, 7), storeID))
    }
    // salesID, (yearMonth, storeID)

    val dataStore = sc.textFile("src/main/scala/stores_mock.csv")
    val headerStore = dataStore.first()
    val store = dataStore.filter(line => line != headerStore).map(line =>
      (line.split(",")(0).trim().toInt, (line.split(",")(1).trim(), line.split(",")(3).trim())))
    // storeID, (storeName, city)

    val pricePerStore = lineItem.join(product).map{ case (productID, ((salesID, quantity), price)) => (salesID, quantity * price) }.reduceByKey({(x, y) => x+y}) //salesID, totalPrice
    val salesPerStore = sales.join(pricePerStore).
      map{ case (salesID, ((yearMonth, storeID), totalPrice)) => ((storeID, yearMonth), totalPrice) }.
      reduceByKey({(x, y) => x+y}).
      map{ case ((storeID, yearMonth), totalSales) => (storeID, (yearMonth, totalSales))}.
      join(store).
      map{ case (storeID, ((yearMonth, totalSales), (storeName, storeCity))) => (yearMonth, (storeName, storeCity, totalSales))}
    // yearMonth, (storeName, storeCity, totalSales)

    val result = salesPerStore.groupByKey().mapValues(iter => {
        // Sort the stores in descending order by totalSales and take top 10
        iter.toList.sortBy(-_._3).take(10)
      })
      .sortByKey() // Sort by yearMonth in ascending order

    result.foreach { case (yearMonth, stores) =>
      println(s"$yearMonth, ${stores.map { case (name, city, sales) => s"($name, $city, $$$sales)" }.mkString(", ")}")
    }

  }
}