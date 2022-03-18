package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
object DataFrame {

    

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .appName("Spark TP")
                  .config("spark.some.config.option", "some-value")
                  .getOrCreate()
  val df =  spark.read.option("delimiter", ";").option("header",true).csv("./data/laposte_hexasmal.csv")
  df.printSchema()
  val count = df.select("Nom_commune").distinct().count()
  println(count)
  val s = df.select("Nom_commune").filter("Ligne_5 is not null").distinct().count()
  print(s)
  val df1= df.withColumn("Departement", col("code_postal").substr(1,2))

  println(" \n Table avec département : créé \n")

// Ecrivez le résultat dans un nouveau fichier CSV nommé “commune_et_departement.csv”, ayant pour colonne Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.

  df.withColumn("Departement", col("code_postal").substr(1,2)).sort("code_postal").write.option("header",true).csv("Nom_commune.csv")

// Affichez les communes du département de l’Aisne.

  println(" \n Communes dans l'Aisne \n")

  df1.filter("Departement = 1").show()

// Quel est le département avec le plus de communes ?

  println(" \n Departement avec le plus de communes \n")

  df1.groupBy("Departement").count().sort(desc("count")).show()


  }


}
