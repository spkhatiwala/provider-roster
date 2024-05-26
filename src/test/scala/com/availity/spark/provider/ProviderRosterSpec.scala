package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }

  describe("Question1") {
    it("should have 15 distinct Speciality counts for dataframe in Question1") {
      val providerRoster = ProviderRoster
      val spark = providerRoster.getSparkSession
      val providerSchema = providerRoster.getProviderSchema
      val visitSchema = providerRoster.getVisitSchema
      val providerDF = providerRoster.readProviderDataframe(spark, providerSchema)
      val visitDF = providerRoster.readVisitDataframe(spark, visitSchema)

      val visitsPerProviderDF = providerRoster.question1DF(providerDF, visitDF)
      val distinctSpecialityCount =  visitsPerProviderDF.select("provider_specialty").distinct.count
      assert(visitsPerProviderDF.count == 1000)
      assert(distinctSpecialityCount == 15)
    }
  }


  describe("Question2") {
    it("should  have 13 distinct months for dataframe in Question2") {
      val providerRoster = ProviderRoster
      val spark = providerRoster.getSparkSession
      val visitSchema = providerRoster.getVisitSchema
      val visitDF = providerRoster.readVisitDataframe(spark, visitSchema)

      val visitsPerProviderPerMonthDF = providerRoster.question2DF( visitDF)
      val distinctMonthCount = visitsPerProviderPerMonthDF.select("month").distinct.count
      assert(distinctMonthCount == 13)
    }
  }
}
