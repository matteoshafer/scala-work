import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object lab6 {
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

    val pricePerSale = lineItem.join(product).map{ case (productID, ((salesID, quantity), price)) => (salesID, quantity * price) }.reduceByKey({(x, y) => x+y}) //salesID, totalPrice

    //    salesPrice.collect.foreach(println)

    val dataSales = sc.textFile("src/main/scala/sales.csv")
    val headerSales = dataSales.first()
    val sales = dataSales.filter(line => line != headerSales).map(line =>
      (line.split(",")(0).trim().toInt, line.split(",")(3).trim().toInt))
    // salesID, storeID

    val salesPerStore = sales.join(pricePerSale).map{ case (salesID, (storeID, totalPrice)) => (storeID, totalPrice) }.reduceByKey({(x, y) => x+y}) //storeID, totalPrice

    val dataStore = sc.textFile("src/main/scala/stores_mock.csv")
    val headerStore = dataStore.first()
    val store = dataStore.filter(line => line != headerStore).map(line =>
      (line.split(",")(0).trim().toInt, line.split(",")(5).trim()))
    // storeID, state

    val salesPerStoreAndState = store.join(salesPerStore).map{ case (storeID, (state, totalPrice)) => (state, storeID, totalPrice) }.sortBy(record => (record._1, record._2))


    salesPerStoreAndState.collect.foreach(println)

  }


}