package Lab5

import scala.io.Source
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection._

case class Store(state: String, storeID: Int, totalSales: Double){
  override def toString:String = {
    return state + ", " + storeID + ", " + totalSales
  }
}


object lab5 {
  def main(args: Array[String]):Unit = {
    val linesLineItem = Source.fromFile("src/main/scala/lineItem_mock.csv").getLines.drop(1)
    val linesProduct = Source.fromFile("src/main/scala/product_mock.csv").getLines.drop(1)
    val linesSales = Source.fromFile("src/main/scala/sales.csv").getLines.drop(1)
    val linesStore = Source.fromFile("src/main/scala/stores_mock.csv").getLines.drop(1)

    val products = mutable.Map[Int, Double]() // productID, price per
    for (line <- linesProduct){
      val tokens = line.split(",")
      products += (tokens(0).toInt -> tokens(2).toDouble)
    }

    val salePrice = mutable.Map[Int, Double]() // salesID, total price
    for (line <- linesLineItem){
      val tokens = line.split(",")
      val salesID = tokens(2).toInt
      val productID = tokens(3).toInt
      val quantity = tokens(4).toInt
      val totalPrice = products(productID) * quantity

      if (!salePrice.contains(salesID)) {
        salePrice += (salesID -> totalPrice)
      } else{
        salePrice(salesID) = salePrice(salesID) + totalPrice
      }
    }

    val TotalSalesPerStore = mutable.Map[Int, Double]() //storeID, total sales
    for (line <- linesSales){
      val tokens = line.split(",")
      val salesID = tokens(0).toInt
      val storeID = tokens(3).toInt

      if (!TotalSalesPerStore.contains(storeID)) {
        TotalSalesPerStore += (storeID -> salePrice(salesID)) // add new record(storeID and totalPrice)
      } else{
        TotalSalesPerStore(storeID) = TotalSalesPerStore(storeID) + salePrice(salesID) // add totalPrice to existing record
      }
    }

    val FinalStoreSales = mutable.Map[Int, Store]() //state, (storeID, total sales)
    for (line <- linesStore){
      val tokens = line.split(",")
      val storeID = tokens(0).toInt
      val state = tokens(5)
      val totalSales = TotalSalesPerStore(storeID)
      val store = new Store(state, storeID, totalSales)
      FinalStoreSales += (storeID -> store)
    }

    FinalStoreSales.values.toList.sortBy(_.state).foreach(println(_))

  }
}

