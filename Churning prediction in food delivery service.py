# Databricks notebook source
filePath = "/FileStore/tables/GroupData/" #put your own file path if necessary

#Importing the files one by one
Complaints = spark.read\
  .format("csv")\
  .option("inferSchema","true")\
  .option("header","true")\
  .option("delimiter",",")\
  .option("0","NA")\
  .load(filePath + "BDT2_1920_Complaints.csv")\

Delivery=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Delivery.csv")

Subscriptions=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Subscriptions.csv")

Customers=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Customers.csv")

Formula=spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("delimiter",",")\
  .load(filePath + "BDT2_1920_Formula.csv")

# COMMAND ----------

from pyspark.sql.functions import *

#Replacing NA in Complaints
#replacing NA with meaningfull value when possible.
#unknown ID will take the value 0
#unknown numeric values like quantities will take the value 999
#NA values in string type column will take a "NA" value or a "no response"/"no solution" depending on the context

#Replacing NA in Complaints
Complaints = Complaints.withColumn("ProductID", when(Complaints["ProductID"] == "NA", 0).otherwise(Complaints["ProductID"]))\
  .withColumn("ProductName", when(Complaints["ProductName"] == "NA", "NA").otherwise(Complaints["ProductName"]))\
  .withColumn("FeedbackTypeID", when(Complaints["FeedbackTypeID"] == "NA", 0).otherwise(Complaints["FeedbackTypeID"]))\
  .withColumn("FeedbackTypeDesc", when(Complaints["FeedbackTypeDesc"] == "NA", "no response").otherwise(Complaints["FeedbackTypeDesc"]))\
  .withColumn("SolutionTypeID", when(Complaints["SolutionTypeID"] == "NA", 0).otherwise(Complaints["SolutionTypeID"]))\
  .withColumn("SolutionTypeDesc", when(Complaints["SolutionTypeDesc"] == "NA", "no solution").otherwise(Complaints["SolutionTypeDesc"]))

#Replacing NA in Delivery
Delivery = Delivery.na.fill("NA", "DeliveryClass")

#Replacing NA in Subscriptions
#NbrMeals_EXCEP NAs have been replaced by the mean NbrMeals_EXCEP ordered by the same NbrMeals_REG category
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==76), 12).otherwise(Subscriptions["NbrMeals_EXCEP"]))
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==304), 13).otherwise(Subscriptions["NbrMeals_EXCEP"]))
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==329), 17).otherwise(Subscriptions["NbrMeals_EXCEP"]))
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",when((Subscriptions["NbrMeals_EXCEP"] == "NA") & (Subscriptions["NbrMeals_REG"]==152), 13).otherwise(Subscriptions["NbrMeals_EXCEP"]))

#RenewalDate 1 and 0 (so if a client renewed 6 times his subscription, the value can be summed to 6)
Subscriptions = Subscriptions.withColumn("RenewalDate",when(Subscriptions["RenewalDate"] == "NA",0).otherwise(1))

#PaymentDate Redondant with PaymentStatus
  #GrossFormulaPrice
  #NetFormulaPrice
  #NbrMealsPrice
  #ProductDiscount
  #FormulaDiscount
  #TotalDiscount
  #TotalPrice
  #TotalCredit
  #All of those are codependent. Maybe after grouping the NbrMeals_REG/EXCEP features, we can replace NA's by the mean of 
  # the category they belong to


# COMMAND ----------

#Complaints
Complaints = Complaints.withColumn("ProductID", Complaints["ProductID"].cast("integer"))\
  .withColumn("SolutionTypeID", Complaints["SolutionTypeID"].cast("integer"))\
  .withColumn("FeedbackTypeID", Complaints["FeedbackTypeID"].cast("integer"))

#Subscriptions
Subscriptions = Subscriptions.withColumn("NbrMeals_EXCEP",Subscriptions["NbrMeals_EXCEP"].cast("integer"))
Subscriptions = Subscriptions.withColumn("RenewalDate",Subscriptions["RenewalDate"].cast("integer"))
  #converting timestamps to number of days
Subscriptions = Subscriptions.withColumn("EndDate",Subscriptions["EndDate"].cast("long")/86400)
Subscriptions = Subscriptions.withColumn("StartDate",Subscriptions["StartDate"].cast("long")/86400)
Complaints = Complaints.withColumn("ComplaintDate",Complaints["ComplaintDate"].cast("long")/86400)

# COMMAND ----------

#Subscriptions
Subscriptions = Subscriptions.withColumn("SubscriptionDuration", Subscriptions.EndDate - Subscriptions.StartDate)
Subscriptions = Subscriptions.withColumn("NbrMealsPerDay", when(Subscriptions["SubscriptionDuration"] == 0, Subscriptions.NbrMeals_REG).otherwise(Subscriptions.NbrMeals_REG / Subscriptions.SubscriptionDuration))

# COMMAND ----------

Subscriptions.createOrReplaceTempView("subscriptions")

# COMMAND ----------

SubInter = spark.sql("select CustomerID, sum(NbrMeals_REG) as TotalMeal_REG, avg(NbrMeals_REG) as MeanMeal_REGPerSub, sum(NbrMeals_EXCEP) as TotalMeal_EXCEP, avg(NbrMeals_EXCEP) as MeanMeal_EXCEPPerSub, min(StartDate) as FirstSubDate, max(EndDate) as EndOfLastSub, (max(EndDate)-min(StartDate)) as HasBeenClientForXDays,count(SubscriptionID) as NbrSub, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END) as SubPaid, SUM(CASE WHEN PaymentStatus='Not Paid' THEN 1 ELSE 0 END) as SubNotPaid, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END)/count(SubscriptionID) as ProportionPaidSub,avg(NbrMealsPrice) as AvgPricePerMeal, sum(ProductDiscount) as TotalProductDiscount, sum(FormulaDiscount) as TotalFormulaDiscount, sum(TotalDiscount) as TotalDiscount, sum(TotalPrice) as TotalPrice, sum(TotalCredit) as TotalCredit,sum(SubscriptionDuration) as NbrDaysSub, avg(SubscriptionDuration) as AvgDurationPerSub, avg(NbrMealsPerDay) as AverageNbrMealPerDay, SUM(CASE WHEN ProductName='Custom Events' THEN 1 ELSE 0 END) as NbrCustomEventsProduct, SUM(CASE WHEN ProductName!='Custom Events' THEN 1 ELSE 0 END) as NbrGrubProduct from subscriptions group by CustomerID")
SubInter = SubInter.withColumn("FirstSubDate", SubInter.FirstSubDate*86400)
SubInter = SubInter.withColumn("FirstSubDate", SubInter.FirstSubDate.cast("timestamp"))
SubInter = SubInter.withColumn("EndOfLastSub", SubInter.EndOfLastSub*86400)
SubInter = SubInter.withColumn("EndOfLastSub", SubInter.EndOfLastSub.cast("timestamp"))
#SubInter = SubInter.withColumn("FirstSubDate", SubInter.select((unix_timestamp("FirstSubDate","yyy/MM/dd HH:mm:ss")).cast("timestamp")))
#df.select((unix_timestamp($"Date", "MM/dd/yyyy HH:mm:ss") * 1000).cast("timestamp"), $"Date")

#creating the churn dependent variable
SubInter = SubInter.withColumn("NotActiveToday", when(col("EndOfLastSub") > "2019-02-02 00:00:00", 0).otherwise(1))
SubInter = SubInter.withColumn("NotActive6MonthAgo", when(col("EndOfLastSub") > "2018-08-02 00:00:00", 0).otherwise(1))
SubInter = SubInter.withColumn("NotActive12MonthAgo", when(col("EndOfLastSub") > "2018-02-02 00:00:00", 0).otherwise(1))

# COMMAND ----------

Complaints.createOrReplaceTempView("complaints")

# COMMAND ----------

Intermediary = spark.sql("select CustomerID, count(ComplaintID) as NbrComplaints, max(ComplaintDate) as LastComplaint, min(ComplaintDate) as FirstComplaint, (CASE WHEN count(ComplaintID)>1 THEN (count(ComplaintID)/(max(ComplaintDate)-min(ComplaintDate))) ELSE 0 END) as ComplaintsPerMonth, SUM(CASE WHEN ProductID=1 THEN 1 ELSE 0 END) as NbrComplaintsProduct1, SUM(CASE WHEN ProductID=2 THEN 1 ELSE 0 END) as NbrComplaintsProduct2, SUM(CASE WHEN ProductID=3 THEN 1 ELSE 0 END) as NbrComplaintsProduct3, SUM(CASE WHEN ProductID=4 THEN 1 ELSE 0 END) as NbrComplaintsProduct4, SUM(CASE WHEN ProductID=5 THEN 1 ELSE 0 END) as NbrComplaintsProduct5, SUM(CASE WHEN ProductID=6 THEN 1 ELSE 0 END) as NbrComplaintsProduct6, SUM(CASE WHEN ProductID=7 THEN 1 ELSE 0 END) as NbrComplaintsProduct7 , SUM(CASE WHEN ProductID=8 THEN 1 ELSE 0 END) as NbrComplaintsProduct8, SUM(CASE WHEN ProductID=0 THEN 1 ELSE 0 END) as NbrComplaintsProductUnknown,SUM(CASE WHEN ComplaintTypeID=1 THEN 1 ELSE 0 END) as NbrComplaintsType1, SUM(CASE WHEN ComplaintTypeID=2 THEN 1 ELSE 0 END) as NbrComplaintsType2, SUM(CASE WHEN ComplaintTypeID=3 THEN 1 ELSE 0 END) as NbrComplaintsType3, SUM(CASE WHEN ComplaintTypeID=4 THEN 1 ELSE 0 END) as NbrComplaintsType4, SUM(CASE WHEN ComplaintTypeID=5 THEN 1 ELSE 0 END) as NbrComplaintsType5, SUM(CASE WHEN ComplaintTypeID=6 THEN 1 ELSE 0 END) as NbrComplaintsType6, SUM(CASE WHEN ComplaintTypeID=7 THEN 1 ELSE 0 END) as NbrComplaintsType7 , SUM(CASE WHEN ComplaintTypeID=8 THEN 1 ELSE 0 END) as NbrComplaintsType8, SUM(CASE WHEN ComplaintTypeID=9 THEN 1 ELSE 0 END) as NbrComplaintsType9, SUM(CASE WHEN ComplaintTypeID=0 THEN 1 ELSE 0 END) as NbrComplaintsTypeUnknown, SUM(CASE WHEN SolutionTypeID=1 THEN 1 ELSE 0 END) as NbrSolutionsType1, SUM(CASE WHEN SolutionTypeID=2 THEN 1 ELSE 0 END) as NbrSolutionsType2, SUM(CASE WHEN SolutionTypeID=3 THEN 1 ELSE 0 END) as NbrSolutionsType3, SUM(CASE WHEN SolutionTypeID=4 THEN 1 ELSE 0 END) as NbrSolutionsType4, SUM(CASE WHEN SolutionTypeID=0 THEN 1 ELSE 0 END) as NbrSolutionsTypeUnknown from complaints group by CustomerID")
Intermediary = Intermediary.withColumn("FirstComplaint", Intermediary.FirstComplaint*86400)
Intermediary = Intermediary.withColumn("FirstComplaint", Intermediary.FirstComplaint.cast("timestamp"))
Intermediary = Intermediary.withColumn("LastComplaint", Intermediary.LastComplaint*86400)
Intermediary = Intermediary.withColumn("LastComplaint", Intermediary.LastComplaint.cast("timestamp"))

# COMMAND ----------

#Base Table
#base = Customers.join(Complaints,on=['CustomerID'],how='full')
#base = Customers.join(Subscriptions,on=['CustomerID'],how='full')
base = Customers.join(Intermediary,on=['CustomerID'],how='full')
base1 = base.join(SubInter,on=['CustomerID'],how='full')
#Replacing null in Complaints
Base = base1.na.fill(0)

# COMMAND ----------

display(Base)

# COMMAND ----------

#Creating the Train table, aka a table that only includes events that occured before the 03/08/2018.

#Subscriptions
ValidationSubInter = spark.sql("select CustomerID, sum(NbrMeals_REG) as TotalMeal_REG, avg(NbrMeals_REG) as MeanMeal_REGPerSub, sum(NbrMeals_EXCEP) as TotalMeal_EXCEP, avg(NbrMeals_EXCEP) as MeanMeal_EXCEPPerSub, min(StartDate) as FirstSubDate, max(EndDate) as EndOfLastSub, (max(EndDate)-min(StartDate)) as HasBeenClientForXDays,count(SubscriptionID) as NbrSub, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END) as SubPaid, SUM(CASE WHEN PaymentStatus='Not Paid' THEN 1 ELSE 0 END) as SubNotPaid, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END)/count(SubscriptionID) as ProportionPaidSub,avg(NbrMealsPrice) as AvgPricePerMeal, sum(ProductDiscount) as TotalProductDiscount, sum(FormulaDiscount) as TotalFormulaDiscount, sum(TotalDiscount) as TotalDiscount, sum(TotalPrice) as TotalPrice, sum(TotalCredit) as TotalCredit,sum(SubscriptionDuration) as NbrDaysSub, avg(SubscriptionDuration) as AvgDurationPerSub, avg(NbrMealsPerDay) as AverageNbrMealPerDay, SUM(CASE WHEN ProductName='Custom Events' THEN 1 ELSE 0 END) as NbrCustomEventsProduct, SUM(CASE WHEN ProductName!='Custom Events' THEN 1 ELSE 0 END) as NbrGrubProduct from subscriptions WHERE StartDate < 17746 group by CustomerID")
ValidationSubInter = ValidationSubInter.withColumn("FirstSubDate", ValidationSubInter.FirstSubDate*86400)
ValidationSubInter = ValidationSubInter.withColumn("FirstSubDate", ValidationSubInter.FirstSubDate.cast("timestamp"))
ValidationSubInter = ValidationSubInter.withColumn("EndOfLastSub", ValidationSubInter.EndOfLastSub*86400)
ValidationSubInter = ValidationSubInter.withColumn("EndOfLastSub", ValidationSubInter.EndOfLastSub.cast("timestamp"))

#creating the churn dependent variable
ValidationSubInter = ValidationSubInter.withColumn("NotActive12MonthAgo", when(col("EndOfLastSub") > "2017-08-02 00:00:00", 0).otherwise(1))
ValidationSubInter = ValidationSubInter.withColumn("NotActive6MonthAgo", when(col("EndOfLastSub") > "2018-02-02 00:00:00", 0).otherwise(1))
ValidationSubInter = ValidationSubInter.withColumn("NotActiveToday", when(col("EndOfLastSub") > "2018-08-02 00:00:00", 0).otherwise(1))
TargetChurn = SubInter["CustomerID","NotActiveToday"]
TargetChurn = TargetChurn.withColumnRenamed("NotActiveToday","ChurnIn6Month")
ValidationSubInter = ValidationSubInter.join(TargetChurn,on=['CustomerID'],how='left')

#Creating the Train table, aka a table that only includes events that occured before the 03/08/2018.

ValidationComplaints = spark.sql("select CustomerID, count(ComplaintID) as NbrComplaints, max(ComplaintDate) as LastComplaint, min(ComplaintDate) as FirstComplaint, (CASE WHEN count(ComplaintID)>1 THEN (count(ComplaintID)/(max(ComplaintDate)-min(ComplaintDate))) ELSE 0 END) as ComplaintsPerMonth, SUM(CASE WHEN ProductID=1 THEN 1 ELSE 0 END) as NbrComplaintsProduct1, SUM(CASE WHEN ProductID=2 THEN 1 ELSE 0 END) as NbrComplaintsProduct2, SUM(CASE WHEN ProductID=3 THEN 1 ELSE 0 END) as NbrComplaintsProduct3, SUM(CASE WHEN ProductID=4 THEN 1 ELSE 0 END) as NbrComplaintsProduct4, SUM(CASE WHEN ProductID=5 THEN 1 ELSE 0 END) as NbrComplaintsProduct5, SUM(CASE WHEN ProductID=6 THEN 1 ELSE 0 END) as NbrComplaintsProduct6, SUM(CASE WHEN ProductID=7 THEN 1 ELSE 0 END) as NbrComplaintsProduct7 , SUM(CASE WHEN ProductID=8 THEN 1 ELSE 0 END) as NbrComplaintsProduct8, SUM(CASE WHEN ProductID=0 THEN 1 ELSE 0 END) as NbrComplaintsProductUnknown,SUM(CASE WHEN ComplaintTypeID=1 THEN 1 ELSE 0 END) as NbrComplaintsType1, SUM(CASE WHEN ComplaintTypeID=2 THEN 1 ELSE 0 END) as NbrComplaintsType2, SUM(CASE WHEN ComplaintTypeID=3 THEN 1 ELSE 0 END) as NbrComplaintsType3, SUM(CASE WHEN ComplaintTypeID=4 THEN 1 ELSE 0 END) as NbrComplaintsType4, SUM(CASE WHEN ComplaintTypeID=5 THEN 1 ELSE 0 END) as NbrComplaintsType5, SUM(CASE WHEN ComplaintTypeID=6 THEN 1 ELSE 0 END) as NbrComplaintsType6, SUM(CASE WHEN ComplaintTypeID=7 THEN 1 ELSE 0 END) as NbrComplaintsType7 , SUM(CASE WHEN ComplaintTypeID=8 THEN 1 ELSE 0 END) as NbrComplaintsType8, SUM(CASE WHEN ComplaintTypeID=9 THEN 1 ELSE 0 END) as NbrComplaintsType9, SUM(CASE WHEN ComplaintTypeID=0 THEN 1 ELSE 0 END) as NbrComplaintsTypeUnknown, SUM(CASE WHEN SolutionTypeID=1 THEN 1 ELSE 0 END) as NbrSolutionsType1, SUM(CASE WHEN SolutionTypeID=2 THEN 1 ELSE 0 END) as NbrSolutionsType2, SUM(CASE WHEN SolutionTypeID=3 THEN 1 ELSE 0 END) as NbrSolutionsType3, SUM(CASE WHEN SolutionTypeID=4 THEN 1 ELSE 0 END) as NbrSolutionsType4, SUM(CASE WHEN SolutionTypeID=0 THEN 1 ELSE 0 END) as NbrSolutionsTypeUnknown FROM complaints WHERE ComplaintDate < 17746  group by CustomerID")
ValidationComplaints = ValidationComplaints.withColumn("FirstComplaint", ValidationComplaints.FirstComplaint*86400)
ValidationComplaints = ValidationComplaints.withColumn("FirstComplaint", ValidationComplaints.FirstComplaint.cast("timestamp"))
ValidationComplaints = ValidationComplaints.withColumn("LastComplaint", ValidationComplaints.LastComplaint*86400)
ValidationComplaints = ValidationComplaints.withColumn("LastComplaint", ValidationComplaints.LastComplaint.cast("timestamp"))

# COMMAND ----------

#Base Table
#base = Customers.join(Complaints,on=['CustomerID'],how='full')
#base = Customers.join(Subscriptions,on=['CustomerID'],how='full')
base = Customers.join(ValidationSubInter,on=['CustomerID'],how='right')
base1 = base.join(ValidationComplaints,on=['CustomerID'],how='full')
#Replacing null in Complaints
BaseValidation = base1.na.fill(0)

display(BaseValidation) #events that occured before the 03/08/2018

# COMMAND ----------

#Creating the Train table, aka a table that only includes events that occured before the 03/02/2018.

#Subscriptions
TrainSubInter = spark.sql("select CustomerID, sum(NbrMeals_REG) as TotalMeal_REG, avg(NbrMeals_REG) as MeanMeal_REGPerSub, sum(NbrMeals_EXCEP) as TotalMeal_EXCEP, avg(NbrMeals_EXCEP) as MeanMeal_EXCEPPerSub, min(StartDate) as FirstSubDate, max(EndDate) as EndOfLastSub, (max(EndDate)-min(StartDate)) as HasBeenClientForXDays,count(SubscriptionID) as NbrSub, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END) as SubPaid, SUM(CASE WHEN PaymentStatus='Not Paid' THEN 1 ELSE 0 END) as SubNotPaid, SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END)/count(SubscriptionID) as ProportionPaidSub,avg(NbrMealsPrice) as AvgPricePerMeal, sum(ProductDiscount) as TotalProductDiscount, sum(FormulaDiscount) as TotalFormulaDiscount, sum(TotalDiscount) as TotalDiscount, sum(TotalPrice) as TotalPrice, sum(TotalCredit) as TotalCredit,sum(SubscriptionDuration) as NbrDaysSub, avg(SubscriptionDuration) as AvgDurationPerSub, avg(NbrMealsPerDay) as AverageNbrMealPerDay, SUM(CASE WHEN ProductName='Custom Events' THEN 1 ELSE 0 END) as NbrCustomEventsProduct, SUM(CASE WHEN ProductName!='Custom Events' THEN 1 ELSE 0 END) as NbrGrubProduct from subscriptions WHERE StartDate < 17565 group by CustomerID")
TrainSubInter = TrainSubInter.withColumn("FirstSubDate", TrainSubInter.FirstSubDate*86400)
TrainSubInter = TrainSubInter.withColumn("FirstSubDate", TrainSubInter.FirstSubDate.cast("timestamp"))
TrainSubInter = TrainSubInter.withColumn("EndOfLastSub", TrainSubInter.EndOfLastSub*86400)
TrainSubInter = TrainSubInter.withColumn("EndOfLastSub", TrainSubInter.EndOfLastSub.cast("timestamp"))

#creating the churn dependent variable
TrainSubInter = TrainSubInter.withColumn("NotActive12MonthAgo", when(col("EndOfLastSub") > "2017-02-02 00:00:00", 0).otherwise(1))
TrainSubInter = TrainSubInter.withColumn("NotActive6MonthAgo", when(col("EndOfLastSub") > "2017-08-02 00:00:00", 0).otherwise(1))
TrainSubInter = TrainSubInter.withColumn("NotActiveToday", when(col("EndOfLastSub") > "2018-02-02 00:00:00", 0).otherwise(1))
TargetChurn = SubInter["CustomerID","NotActive6MonthAgo"]
TargetChurn = TargetChurn.withColumnRenamed("NotActive6MonthAgo","ChurnIn6Month")
TrainSubInter = TrainSubInter.join(TargetChurn,on=['CustomerID'],how='left')

#Creating the Train table, aka a table that only includes events that occured before the 03/02/2018.

TrainComplaints = spark.sql("select CustomerID, count(ComplaintID) as NbrComplaints, max(ComplaintDate) as LastComplaint, min(ComplaintDate) as FirstComplaint, (CASE WHEN count(ComplaintID)>1 THEN (count(ComplaintID)/(max(ComplaintDate)-min(ComplaintDate))) ELSE 0 END) as ComplaintsPerMonth, SUM(CASE WHEN ProductID=1 THEN 1 ELSE 0 END) as NbrComplaintsProduct1, SUM(CASE WHEN ProductID=2 THEN 1 ELSE 0 END) as NbrComplaintsProduct2, SUM(CASE WHEN ProductID=3 THEN 1 ELSE 0 END) as NbrComplaintsProduct3, SUM(CASE WHEN ProductID=4 THEN 1 ELSE 0 END) as NbrComplaintsProduct4, SUM(CASE WHEN ProductID=5 THEN 1 ELSE 0 END) as NbrComplaintsProduct5, SUM(CASE WHEN ProductID=6 THEN 1 ELSE 0 END) as NbrComplaintsProduct6, SUM(CASE WHEN ProductID=7 THEN 1 ELSE 0 END) as NbrComplaintsProduct7 , SUM(CASE WHEN ProductID=8 THEN 1 ELSE 0 END) as NbrComplaintsProduct8, SUM(CASE WHEN ProductID=0 THEN 1 ELSE 0 END) as NbrComplaintsProductUnknown,SUM(CASE WHEN ComplaintTypeID=1 THEN 1 ELSE 0 END) as NbrComplaintsType1, SUM(CASE WHEN ComplaintTypeID=2 THEN 1 ELSE 0 END) as NbrComplaintsType2, SUM(CASE WHEN ComplaintTypeID=3 THEN 1 ELSE 0 END) as NbrComplaintsType3, SUM(CASE WHEN ComplaintTypeID=4 THEN 1 ELSE 0 END) as NbrComplaintsType4, SUM(CASE WHEN ComplaintTypeID=5 THEN 1 ELSE 0 END) as NbrComplaintsType5, SUM(CASE WHEN ComplaintTypeID=6 THEN 1 ELSE 0 END) as NbrComplaintsType6, SUM(CASE WHEN ComplaintTypeID=7 THEN 1 ELSE 0 END) as NbrComplaintsType7 , SUM(CASE WHEN ComplaintTypeID=8 THEN 1 ELSE 0 END) as NbrComplaintsType8, SUM(CASE WHEN ComplaintTypeID=9 THEN 1 ELSE 0 END) as NbrComplaintsType9, SUM(CASE WHEN ComplaintTypeID=0 THEN 1 ELSE 0 END) as NbrComplaintsTypeUnknown, SUM(CASE WHEN SolutionTypeID=1 THEN 1 ELSE 0 END) as NbrSolutionsType1, SUM(CASE WHEN SolutionTypeID=2 THEN 1 ELSE 0 END) as NbrSolutionsType2, SUM(CASE WHEN SolutionTypeID=3 THEN 1 ELSE 0 END) as NbrSolutionsType3, SUM(CASE WHEN SolutionTypeID=4 THEN 1 ELSE 0 END) as NbrSolutionsType4, SUM(CASE WHEN SolutionTypeID=0 THEN 1 ELSE 0 END) as NbrSolutionsTypeUnknown FROM complaints WHERE ComplaintDate < 17565  group by CustomerID")
TrainComplaints = TrainComplaints.withColumn("FirstComplaint", TrainComplaints.FirstComplaint*86400)
TrainComplaints = TrainComplaints.withColumn("FirstComplaint", TrainComplaints.FirstComplaint.cast("timestamp"))
TrainComplaints = TrainComplaints.withColumn("LastComplaint", TrainComplaints.LastComplaint*86400)
TrainComplaints = TrainComplaints.withColumn("LastComplaint", TrainComplaints.LastComplaint.cast("timestamp"))


# COMMAND ----------

#Base Table
#base = Customers.join(Complaints,on=['CustomerID'],how='full')
#base = Customers.join(Subscriptions,on=['CustomerID'],how='full')
base = Customers.join(TrainSubInter,on=['CustomerID'],how='right')
base1 = base.join(TrainComplaints,on=['CustomerID'],how='full')
#Replacing null in Complaints
BaseTrain = base1.na.fill(0)

display(BaseTrain) #events that occured before the 03/02/2018

# COMMAND ----------

#trial = basetable_train.toPandas()
trial = BaseTrain.toPandas()


# COMMAND ----------

#trial.info()
trial = trial.drop(["Region","StreetID","FirstSubDate","EndOfLastSub","LastComplaint","FirstComplaint"],axis = 1)

# COMMAND ----------

correl = trial.corr()

# COMMAND ----------

correl.columns

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sh pip install --upgrade numpy

# COMMAND ----------

display(trial)

# COMMAND ----------

#selecting the columns based on the correlation
cols = np.full((correl.shape[0],), True, dtype=bool)
for i in range(correl.shape[0]):
    for j in range(i+1, correl.shape[0]):
        if correl.iloc[i,j] >= 0.3:
            if cols[j]:
                cols[j] = False
selected_columns = trial.columns[cols]
data = trial[selected_columns]

# COMMAND ----------

Tomerge = BaseTrain.toPandas()
ToMerge = Tomerge[["Region","CustomerID","StreetID","ChurnIn6Month"]]
data = data.merge(ToMerge,left_on=["CustomerID"],right_on=["CustomerID"])

# COMMAND ----------

data1 = spark.createDataFrame(data)


# COMMAND ----------

#Create categorical variables for Region and StreetID 
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer
from pyspark.ml import Pipeline




#Region
genderIndxr = StringIndexer().setInputCol("Region").setOutputCol("RegionInd")

#StreetID
classIndxr = StringIndexer().setInputCol("StreetID").setOutputCol("StreetIDInd")

#One-hot encoding
ohee_catv = OneHotEncoderEstimator(inputCols=["RegionInd","StreetIDInd"],outputCols=["Region_dum","StreetID_dum"])
pipe_catv = Pipeline(stages=[genderIndxr, classIndxr, ohee_catv])

basetable_train = pipe_catv.fit(data1).transform(data1)
basetable_train = basetable_train.drop("RegionInd","StreetIDInd","FirstSubDate","LastComplaint","FirstComplaint","ChurnedAt03/02/2018","ChurnedAt03/02/2019")


basetable_train= basetable_train.withColumnRenamed("ChurnIn6Month","label")

# COMMAND ----------

basetable_test = pipe_catv.fit(BaseValidation).transform(BaseValidation)
basetable_test = basetable_test.drop("RegionInd","StreetIDInd","FirstSubDate","LastComplaint","FirstComplaint","ChurnedAt03/02/2018","ChurnedAt03/02/2019")


basetable_test= basetable_test.withColumnRenamed("ChurnIn6Month","label")

# COMMAND ----------



# COMMAND ----------

#Transform the tables in a table of label, features format
from pyspark.ml.feature import RFormula

#trainBig = RFormula(formula="label ~ . - CustomerID").fit(basetable_final).transform(basetable_final)
train = RFormula(formula="label ~ . - CustomerID").fit(basetable_train).transform(basetable_train)
test = RFormula(formula="label ~ . - CustomerID").fit(basetable_test).transform(basetable_test)
#print("trainBig nobs: " + str(trainBig.count()))
print("train nobs: " + str(train.count()))
print("test nobs: " + str(test.count()))

# COMMAND ----------

display(train)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

#Define pipeline
rfc = RandomForestClassifier()
rfPipe = Pipeline().setStages([rfc])

evaluator = BinaryClassificationEvaluator()


#Set param grid
rfParams = ParamGridBuilder()\
  .addGrid(rfc.numTrees, [150, 300, 500])\
  .addGrid(rfc.maxDepth, [1,2,3])\
  .build()

rfCv = CrossValidator()\
  .setEstimator(rfPipe)\
  .setEstimatorParamMaps(rfParams)\
  .setEvaluator(evaluator)\
  .setNumFolds(5) # Here: 5-fold cross validation

#Run cross-validation, and choose the best set of parameters.
rfcModel = rfCv.fit(train)

# COMMAND ----------

rfcBestModel = rfcModel.bestModel.stages[-1] #-1 means "get last stage in the pipeline"

# COMMAND ----------

#Prettify feature importances
import pandas as pd
def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    return(varlist.sort_values('score', ascending = False))
  
ExtractFeatureImp(rfcBestModel.featureImportances, train, "features").head(10)

# COMMAND ----------

#Get predictions on the test set
preds = rfcModel.transform(test)
preds1 = rfcModel.transform(test)\
  .select("prediction", "label","CustomerID","probability")

preds.show(10)

display(preds)

# COMMAND ----------


from pyspark.mllib.evaluation import BinaryClassificationMetrics

#Get model accuracy
print("accuracy: " + str(evaluator.evaluate(preds)))

#Get AUC
metrics = BinaryClassificationMetrics(preds1.rdd.map(lambda x: (float(x[0]), float(x[1]))))
print("AUC: " + str(metrics.areaUnderROC))
