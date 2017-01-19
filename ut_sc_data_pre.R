# DATA PREPROCESSING FOR UNITRADE MERCHANDISE -- SUPPLY CHAIN
# =====================================================
# Introduction:
# This program is written to preprocess the Supply Chain data from Oracle for Unitrade Merchandise.
# The preprocess involves arranging the columns and summing the appropriate rows to be used in Excel pivot table
# =====================================================
# Developed by: jt.analytics@gmail.com
# Ver 0.1
# Date: 01/19/2017
# =====================================================
# Requirements:
# 1. CSV outputs from Oracle that contain Supply Chain data with these headers:
    #NET_OF_RTN_CASE	CLASS_NAME	CATEGORY	INVENTORY_ID	DESCRIPTION	BOOKED_CASE_QTY	SOLOMON_REGION	NET_OF_DISC_RTNS_PESO_SALES	SOLOMON_CHANNEL	SOLOMON_TERRITORY	ORDERED_MONTH	ORDERED_YEAR	ORDERED_DATE	NET_OF_RETURNS_PADS_SALES
# 2. No other CSV files must be in the working directory!!
# =====================================================
# Procedure:
# 1. Initialize data table with correct field names
    #MONTH
    #WEEK 
    #DATE 
    #SOLOMON_REGION 
    #CATEGORY 
    #INVENTORY_ID
    #DESCRIPTION 
    #CASE_QTY 
    #PADS_QTY 
    #TRANSACTION_TYPE = "CY booked sales"
# LOOP STARTS HERE
# 2. Read next CSV
# 3. Using KEYS, find any matching rows
# 3a. If there are matching rows, update via reference. Save the matching rows of original dataset into temp data.table
# 3b. If there are no matching rows, do not update view reference. Save the matching rows of original dataset into temp data.table (should be empty)
# 4. rbindlist the original dataset WITHOUT the matching rows (use ! notation)
# temp <- final[!x[,INVENTORY_ID:ORDERED_DATE]]
# REPEAT FOR NEXT CSV UNTIL ALL CSV's ARE READ
# =====================================================
#install.packages("dtplyr")
#install.packages("data.table")
#install.packages("stringi")
library(dtplyr)
library(data.table)
library(stringi)
save_output <- 1    # 1 - enabled, 0 - disabled

# 1. Initialize data table with correct field names
final_dt <- data.table( #MONTH = character(),
                        #WEEK = integer(),
                        ORDERED_DATE = as.Date(character()),
                        SOLOMON_REGION = character(),
                        CATEGORY = character(),
                        INVENTORY_ID = character(),
                        DESCRIPTION = character(),
                        CASE_QTY = double(),
                        PADS_QTY = integer()
                        #TRANSACTION_TYPE = character()
                        )
setkey(final_dt, INVENTORY_ID, SOLOMON_REGION, ORDERED_DATE)

# LOOP STARTS HERE
# 2. Read next CSV
# List down all of the CSV's in the directory
listofcsv = list.files(pattern="*.csv")
for (csvfile in listofcsv){
    dataset <- fread(csvfile)
    #dataset <- fread("edited_oracle_snop_raw2.csv")
    #dataset <- fread("oracle_snop_raw2.csv")
    dataset[,ORDERED_DATE:=as.Date(ORDERED_DATE,"%m/%d/%Y")]

    # 3. Using KEYS, find any matching rows
    
    # Create a data.table that sums up the total NET_OF_RTN_CASE and sums up the total NET_OF_RETURNS_PADS_SALES
    # Grouped By CATEGORY, INVENTORY_ID, DESCRIPTION, SOLOMON_REGION, ORDERED_DATE
    ans <- dataset[, .(CASE_QTY = sum(NET_OF_RTN_CASE), PADS_QTY = sum(NET_OF_RETURNS_PADS_SALES)), 
                   by = .(CATEGORY, INVENTORY_ID, DESCRIPTION, SOLOMON_REGION, ORDERED_DATE)
                   ][order(CATEGORY, INVENTORY_ID, SOLOMON_REGION, ORDERED_DATE)]
    
    # Set the keys to the unique ID's (INVENTORY_ID, SOLOMON_REGION, ORDERED_DATE) that will help us match rows
    setkey(ans, INVENTORY_ID, SOLOMON_REGION, ORDERED_DATE)
    
    # 3a. If there are matching rows, update via reference. Save the matching rows of original dataset into temp data.table
    # 3b. If there are no matching rows, do not update view reference. Save the matching rows of original dataset into temp data.table (should be empty)
    # This should only replace matched rows' old CASE_QTY and PADS_QTY with new ones
    final_dt[ans[,.(INVENTORY_ID,SOLOMON_REGION,ORDERED_DATE)],c("CASE_QTY", "PADS_QTY") := list(ans[,CASE_QTY], ans[,PADS_QTY])]
    temp <- final_dt[ans[,.(INVENTORY_ID,SOLOMON_REGION,ORDERED_DATE)],nomatch = 0L]
    print(dim(temp))
    #temp <- final_dt[ans[,.(INVENTORY_ID,SOLOMON_REGION,ORDERED_DATE)]]

    # 4. rbindlist the original dataset WITHOUT the matching rows (use ! notation)
    temp2 <- ans[!temp[,.(INVENTORY_ID,SOLOMON_REGION,ORDERED_DATE)]]
    l <- list(final_dt, temp2)
    final_dt <- rbindlist(l,use.names=TRUE)
    setkey(final_dt, INVENTORY_ID, SOLOMON_REGION, ORDERED_DATE)
}
    # REPEAT FOR NEXT CSV UNTIL ALL CSV's ARE READ

# Clean up the final database by adding Month, Week, Transaction Type
final_dt[,TRANSACTION_TYPE:="CY Booked Sales"]
final_dt[,MONTH:=format(final_dt[,ORDERED_DATE],"%b")]
final_dt[,WEEK:=stri_datetime_fields(final_dt[,ORDERED_DATE])$WeekOfYear]

final_dt <- final_dt[, .(CASE_QTY = sum(CASE_QTY), PADS_QTY = sum(PADS_QTY)),
                      by = .(MONTH, WEEK, SOLOMON_REGION, CATEGORY, INVENTORY_ID, DESCRIPTION, TRANSACTION_TYPE)][
                          order(WEEK, SOLOMON_REGION, CATEGORY, INVENTORY_ID)]

if (save_output == 1){
    # Save data table into CSV's
    dir1 = "./excel_data_consolidation"
    dir.create(dir1)
    dir2 = "./archive_data_consolidation"
    dir.create(dir2)
    
    filename1 = "excel_data_consolidation.csv"
    fwrite(final_dt, file = file.path(dir1,filename1))
    filename2 = gsub(":","-",paste("archive_",gsub(" ","_",Sys.time()),".csv", sep=""))
    fwrite(final_dt, file = file.path(dir2,filename2))
}
