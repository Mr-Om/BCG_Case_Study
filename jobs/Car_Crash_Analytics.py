from pyspark.sql.functions import *
from pyspark.sql.window import Window
from dependencies.spark import start_spark
from pyspark.sql import Row
import os


def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])
    
    ROOT_DIR  = os.path.abspath(os.curdir)
    DATA_PATH = ROOT_DIR+'/data_files'
    OUT_PATH = ROOT_DIR+'/output_files'

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    unitsDF = createDataFrame(spark,os.path.join(DATA_PATH,config['Units_use']))
    damageDF = createDataFrame(spark,os.path.join(DATA_PATH,config['Damage_use_file_path']))
    chargeDF = createDataFrame(spark,os.path.join(DATA_PATH,config['Charges_use_file_path']))
    endorseDF = createDataFrame(spark,os.path.join(DATA_PATH,config['Endorse_use_file_path']))
    restrictDF = createDataFrame(spark,os.path.join(DATA_PATH,config['Restrict_use']))
    primary_PersonDF = createDataFrame(spark,os.path.join(DATA_PATH,config['Primary_Person_use']))

    #Question_1: Find the number of crashes (accidents) in which number of persons killed are male?

    Question_1DF = Analytics_1(primary_PersonDF)
    load_dataFrame(Question_1DF,os.path.join(OUT_PATH,config['Output1']))

    #Question_2: How many two wheelers are booked for crashes? 
    Question_2DF = Analytics_2(unitsDF)
    load_dataFrame(Question_2DF,os.path.join(OUT_PATH,config['Output2']))

    # Which state has highest number of accidents in which females are involved? 
    Question_3 = Analytics_3(primary_PersonDF)
    load_data(spark,Question_3,os.path.join(OUT_PATH,config['Output3']))

    # Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    Question_4DF = Analytics_4(primary_PersonDF,unitsDF)
    load_dataFrame(Question_4DF,os.path.join(OUT_PATH,config['Output4']))

    # For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    Question_5DF = Analytics_5(primary_PersonDF,unitsDF)
    load_dataFrame(Question_5DF,os.path.join(OUT_PATH,config['Output5']))

    #Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    Question_6 = Analytics_6(primary_PersonDF,unitsDF)
    load_data(spark,Question_6,os.path.join(OUT_PATH,config['Output6']))
    
    #Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    Question_7 = Analytics_7(unitsDF,damageDF)
    load_data(spark,Question_7,os.path.join(OUT_PATH,config['Output7']))

    #: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

    Question_8 = Analytics_8(unitsDF,damageDF)
    load_data(spark,Question_8,os.path.join(OUT_PATH,config['Output8']))



    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def load_data(spark,limit,outPath):
    '''
    This function is helping to write integer/string or list of integer/string to output file
    '''
    (spark\
    .sparkContext\
    .parallelize([limit])\
    .coalesce(1)\
    .saveAsTextFile(outPath))

    return None

def load_dataFrame(df,outPath):
    '''
    This function is helping to write dataframe to output file
    '''
    df\
    .coalesce(1)\
    .write\
    .format('text')\
    .save(outPath)

    return None

# Databricks notebook source
def createDataFrame(spark,path):
    '''
    This function is helping to create dataframe from our data csv files
    '''
    df = spark\
            .read\
            .format("csv")\
            .option("header",True)\
            .option("infershema",True)\
            .load(path)

    return df
  


def Analytics_1(primary_PersonDF):
    '''
    Find the number of crashes (accidents) in which number of persons killed are male?
    '''
    answer_1 = primary_PersonDF\
                .filter(col("PRSN_GNDR_ID")=="MALE")\
                .filter(col("PRSN_INJRY_SEV_ID")=="KILLED")\
                .select(countDistinct("CRASH_ID"))
    
    return answer_1

    #Answer found => 180

def Analytics_2(unitsDF):
    '''
    How many two wheelers are booked for crashes? 
    '''
    answer_2 = unitsDF\
          .filter((col("VEH_BODY_STYL_ID")=="MOTORCYCLE") | (col("VEH_BODY_STYL_ID")=="POLICE MOTORCYCLE"))\
          .select(countDistinct("CRASH_ID"))
    
    return answer_2

    #Answer found => 757

def Analytics_3(primary_PersonDF):
    '''
    Which state has highest number of accidents in which females are involved? 
    '''

    answer_3 = primary_PersonDF\
           .filter(col("PRSN_GNDR_ID")=="FEMALE")\
           .groupBy(col("DRVR_LIC_STATE_ID"))\
           .agg(countDistinct("CRASH_ID").alias("count"))\
           .orderBy("count",ascending=False)\
           .take(1)[0][0]
    
    return answer_3

#Answer found => Texas

def Analytics_4(primary_PersonDF,unitsDF):
    '''
    Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    '''

    Grouped_veh_df = primary_PersonDF.join(unitsDF,primary_PersonDF.CRASH_ID ==  unitsDF.CRASH_ID,"inner") \
                    .filter((primary_PersonDF.PRSN_INJRY_SEV_ID!='NA') & (primary_PersonDF.PRSN_INJRY_SEV_ID!='NOT INJURED') | (primary_PersonDF.PRSN_INJRY_SEV_ID!='UNKNOWN'))\
                    .filter((unitsDF.VEH_MAKE_ID!='NA') & (unitsDF.VEH_MAKE_ID!='UNKNOWN'))\
                    .groupBy(unitsDF.VEH_MAKE_ID)\
                    .agg(countDistinct("CRASH_ID").alias("Injury_Count"))\
                    .orderBy("Injury_Count",ascending=False)

    windowSpec  = Window.orderBy(col("Injury_Count").desc())

    Grouped_veh_df = Grouped_veh_df.withColumn("row_number",row_number().over(windowSpec)) \
        .select("VEH_MAKE_ID")\
        .where((col("row_number")>=5) & (col("row_number")<=15))

    return Grouped_veh_df

'''
Answer found for Question 4

 VEH_MAKE_ID|
+------------+
|      NISSAN|
|       HONDA|
|         GMC|
|        JEEP|
|     HYUNDAI|
|         KIA|
|    CHRYSLER|
|FREIGHTLINER|
|       MAZDA|
|       LEXUS|
|  VOLKSWAGEN|
+------------+

'''
    

def Analytics_5(primary_PersonDF,unitsDF):
    '''
    For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
    '''
    Grouped_body_df = primary_PersonDF.join(unitsDF,primary_PersonDF.CRASH_ID ==  unitsDF.CRASH_ID,"inner") \
                .filter((unitsDF.VEH_BODY_STYL_ID!='NA') & (unitsDF.VEH_BODY_STYL_ID!='NOT REPORTED') & (unitsDF.VEH_BODY_STYL_ID!='UNKNOWN')&
                (primary_PersonDF.PRSN_ETHNICITY_ID!='NA') & (primary_PersonDF.PRSN_ETHNICITY_ID!='UNKNOWN'))\
                .groupBy(unitsDF.VEH_BODY_STYL_ID,primary_PersonDF.PRSN_ETHNICITY_ID)\
                .agg(countDistinct("CRASH_ID").alias("Ethnicity_Count"))\
                

    windowSpec  = Window.partitionBy(unitsDF.VEH_BODY_STYL_ID).orderBy(col("Ethnicity_Count").desc())

    Result_body_df = Grouped_body_df.withColumn("row_number",row_number().over(windowSpec)) \
        .select(unitsDF.VEH_BODY_STYL_ID,primary_PersonDF.PRSN_ETHNICITY_ID,col("Ethnicity_Count"))\
        .where(col("row_number")==1)
    
    return Result_body_df

        
def Analytics_6(primary_PersonDF,unitsDF):
    '''
    Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the 
    contributing factor to a crash (Use Driver Zip Code)
    '''
    Grouped_AlchoholDF = primary_PersonDF.join(unitsDF,primary_PersonDF.CRASH_ID ==  unitsDF.CRASH_ID,"inner") \
                    .filter((col("CONTRIB_FACTR_1_ID") == 'UNDER INFLUENCE - ALCOHOL') | (col("CONTRIB_FACTR_1_ID") == 'HAD BEEN DRINKING') |
                           (col("CONTRIB_FACTR_2_ID") == 'HAD BEEN DRINKING') | (col("CONTRIB_FACTR_2_ID") == 'UNDER INFLUENCE - ALCOHOL') |
                           (col("CONTRIB_FACTR_P1_ID") == 'HAD BEEN DRINKING') | (col("CONTRIB_FACTR_P1_ID") == 'UNDER INFLUENCE - ALCOHOL'))\
                    .filter(primary_PersonDF.DRVR_ZIP!='null')\
                    .groupBy(primary_PersonDF.DRVR_ZIP)\
                    .agg(countDistinct("CRASH_ID").alias("count"))\
                    .orderBy("count",ascending=False)\
                    .take(5)

    Alchohol_Result = []
    for each in Grouped_AlchoholDF:
        Alchohol_Result.append(each[0])
    
    return Alchohol_Result

#Answer 6 => ['78521', '75067', '76010', '78666', '78130']

def Analytics_7(unitsDF,damageDF):
    '''
    Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    '''

    Grouped_DamagedDF = unitsDF.join(damageDF,damageDF.CRASH_ID == unitsDF.CRASH_ID,"left") \
                    .filter((damageDF.DAMAGED_PROPERTY=='NONE') | (damageDF.DAMAGED_PROPERTY=='NONE1') | damageDF.CRASH_ID.isNull())\
                    .filter(unitsDF.FIN_RESP_TYPE_ID!='NA')\
                    .filter((unitsDF.VEH_DMAG_SCL_1_ID=='DAMAGED 5')|(unitsDF.VEH_DMAG_SCL_1_ID=='DAMAGED 6')|(unitsDF.VEH_DMAG_SCL_1_ID=='DAMAGED 7 HIGHEST')|(unitsDF.VEH_DMAG_SCL_2_ID == 'DAMAGED 5') | (unitsDF.VEH_DMAG_SCL_2_ID == 'DAMAGED 6') 
                           | (unitsDF.VEH_DMAG_SCL_2_ID == 'DAMAGED 7 HIGHEST'))\
                    .select(unitsDF.CRASH_ID)\
                    .distinct()\
                    .count()
    
    return Grouped_DamagedDF

#Answer found => 8860

def Analytics_8(unitsDF,endorseDF,primary_PersonDF):
    '''
    Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed 
    Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number 
    of offences (to be deduced from the data)
    '''

    # Find the Unlicenced drivers
    UnlicencedDF = endorseDF.select("CRASH_ID").where(col('DRVR_LIC_ENDORS_ID')=='UNLICENSED')

    
    #Take all the drivers and left join with UnLicenced Driver and filter all the values that are null for UnLicenced Driver.
    
    LicencedDF = unitsDF.join(UnlicencedDF,UnlicencedDF.CRASH_ID == unitsDF.CRASH_ID,"left") \
                    .filter(UnlicencedDF.CRASH_ID.isNull())\
                    .drop(UnlicencedDF.CRASH_ID)

   # Find top 10 vehicle colors
    top10colorDF =unitsDF\
               .filter(col('VEH_COLOR_ID') != 'NA')\
               .groupBy("VEH_COLOR_ID")\
               .agg(countDistinct("CRASH_ID").alias("count"))\
               .orderBy("count",ascending=False)\
               .take(10)

    top10colorList = []

    for each in top10colorDF:
        top10colorList.append(each[0])

    #Find the Top 25 states with highest number of offences 
    Top25OffenceStateDF = primary_PersonDF\
                      .filter((col('DRVR_LIC_STATE_ID')!='NA') & (col('DRVR_LIC_STATE_ID')!='Unknown'))\
                      .groupBy('DRVR_LIC_STATE_ID')\
                      .agg(countDistinct('CRASH_ID').alias("count"))\
                      .orderBy('count',ascending=False)\
                      .take(25)


    Top25OffenceStateList = []
    for each in Top25OffenceStateDF:
        Top25OffenceStateList.append(each[0])

    #Calculating final result
    finalJoinedDF = LicencedDF.join(primary_PersonDF,LicencedDF.CRASH_ID == primary_PersonDF.CRASH_ID)\
            .filter((col("CONTRIB_FACTR_1_ID") == 'FAILED TO CONTROL SPEED') | (col("CONTRIB_FACTR_1_ID") == 'UNSAFE SPEED') | (col("CONTRIB_FACTR_1_ID") == 'SPEEDING - (OVERLIMIT)')|
                           (col("CONTRIB_FACTR_2_ID") == 'FAILED TO CONTROL SPEED') | (col("CONTRIB_FACTR_2_ID") == 'UNSAFE SPEED') | (col("CONTRIB_FACTR_2_ID") == 'SPEEDING - (OVERLIMIT)') |
                           (col("CONTRIB_FACTR_P1_ID") == 'FAILED TO CONTROL SPEED') | (col("CONTRIB_FACTR_P1_ID") == 'UNSAFE SPEED') | (col("CONTRIB_FACTR_P1_ID") == 'SPEEDING - (OVERLIMIT)'))\
  .filter((LicencedDF.VEH_COLOR_ID.isin(top10colorList)) & ((primary_PersonDF.DRVR_LIC_STATE_ID.isin(Top25OffenceStateList))))\
  .groupBy('VEH_MAKE_ID')\
  .agg(countDistinct(LicencedDF.CRASH_ID).alias('count'))\
  .orderBy('count',ascending=False)\
  .take(5)

    answerList = []

    for each in finalJoinedDF:
        answerList.append(each[0])
    
    return answerList

#Ans8 => ['FORD', 'CHEVROLET', 'TOYOTA', 'DODGE', 'HONDA']



# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

