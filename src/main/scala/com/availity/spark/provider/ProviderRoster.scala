package com.availity.spark.provider

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

object ProviderRoster  {

  def getProviderSchema():StructType = {
    StructType(
      Array(
        StructField("provider_id", IntegerType),
        StructField("provider_specialty", StringType),
        StructField("first_name", StringType),
        StructField("middle_name", StringType),
        StructField("last_name", StringType)
      )
    )
  }


  def getVisitSchema():StructType = {
    StructType(
      Array(
        StructField("visit_id", IntegerType),
        StructField("provider_id", IntegerType),
        StructField("service_dt", DateType)
      )
    )
  }

  def getSparkSession():SparkSession= {
    SparkSession.builder()
      .appName("CSV to Dataset")
      .master("local[*]") // Use "local[*]" to run locally with all available cores
      .getOrCreate
  }

  def readProviderDataframe(spark:SparkSession, providerSchema:StructType):DataFrame={
    spark.read
      .schema(providerSchema)
      .option("header", "true") // If your CSV file has a header
      .option("delimiter", "|") // Specify the delimiter
      .csv("data/providers.csv") // Adjust the path accordingly
  }

  def readVisitDataframe(spark:SparkSession, visitSchema:StructType):DataFrame={
    spark.read
      .schema(visitSchema)
      .option("header", "false") // If your CSV file has a header
      .option("delimiter", ",") // Specify the delimiter
      .csv("data/visits.csv") // Adjust the path accordingly
  }

  def question1DF(providerDF:DataFrame, visitDF:DataFrame)={
    val providerVisitDF = providerDF.join(visitDF, "provider_id")
    //aggregate using groupBy provider_id
    val visitsPerProviderDF = providerVisitDF.groupBy(
      "provider_id",
      "provider_specialty",
      "first_name",
      "middle_name",
      "last_name"
    ).count()
    visitsPerProviderDF
  }

  def question2DF(visitDF:DataFrame):DataFrame={
    val visitWithMonthDF = visitDF.withColumn("month", F.date_format(F.col("service_dt"), "yyyy-MM"))

    // Group by provider_id and month, and count visits
    val visitsPerProviderPerMonthDF = visitWithMonthDF.groupBy("provider_id", "month")
      .agg(F.count("visit_id").alias("total_visits"))
    visitsPerProviderPerMonthDF
  }

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession
    val providerSchema = getProviderSchema
    val visitSchema = getVisitSchema

    // Read the Providers CSV file into a DataFrame
    val providerDF = readProviderDataframe(spark,providerSchema)

    // Read the Visits CSV file into a DataFrame
    val visitDF = readVisitDataframe(spark, visitSchema)

    //-------- QUESTION 1---------------------
    val visitsPerProviderDF =  question1DF(providerDF, visitDF)

    //--- Write the result to JSON
    visitsPerProviderDF.write
      .partitionBy("provider_specialty")
      .json("output_path1")
    //-------- QUESTION 1---------------------



    //-------- QUESTION 2---------------------
    val visitsPerProviderPerMonthDF = question2DF(visitDF)

    // Write the result to JSON
    visitsPerProviderPerMonthDF.write
       .json("output_path2")
    //-------- QUESTION 2---------------------





    // Stop the SparkSession
    spark.stop
  }
}
