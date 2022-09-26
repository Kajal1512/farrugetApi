from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from datetime import datetime, timezone
from azure.mgmt.datafactory.models import *
import datetime
from datetime import timedelta
from azure.mgmt.datafactory.models import ScheduleTriggerRecurrence
from azure.mgmt.datafactory.models import TriggerResource

import psycopg2
from azure.cosmos import exceptions, CosmosClient, PartitionKey, cosmos_client
import webbrowser
import requests
import psycopg2
import json
import time
from cryptography.fernet import Fernet
import string
import random
import psycopg2
from azure.cosmos import exceptions, CosmosClient, PartitionKey, cosmos_client
import webbrowser
import requests
import psycopg2
import json
import time
from cryptography.fernet import Fernet
import string
import random  # define the random module


def DecodePassword(password):
    key = b't10dtXImHwWp6pcc7GI0ydkX5l-JPyKV-CLvk8QPQh4='
    fernet = Fernet(key)
    encPassword = fernet.decrypt(password).decode()
    return encPassword


def generationAccesstoken(datasetId, reportId, groupId):
    APPLICATION_ID = "3c2004e3-1b41-4051-aecc-b7f9a4cfb78b"  # The ID of the application in Active Directory
    APPLICATION_SECRET = "1OV8Q~NcExwdXaS90rnPkCdmAZhxjghrLhUencRv"  # A valid key for that application in Active Directory

    USER_ID = "shubhampatil@techmenttech123.onmicrosoft.com"  # A user that has access to PowerBI and the application
    USER_PASSWORD = "c=JcoV-LU%f7mkt-"  # The password for that user
    endpoint = "https://api.powerbi.com/v1.0/myorg/GenerateToken"

    json1 = {
        "datasets": [
            {
                "id": datasetId  # dataset_id   group = workspace in powerBi
            }
        ],
        "reports": [
            {
                "id": reportId
            }
        ],
        "targetWorkspaces": [{"id": groupId}]

    }
    headers = make_headers(APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
    token = requests.post(endpoint, headers=headers, json=json1)
    # print(token.json())
    return token.json()


class LoginAPI(APIView):

    def post(self, request):
        username = request.data["username"]

        password = request.data["password"]
        conn = psycopg2.connect(
            database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
            host='aristanalyticspoc.postgres.database.azure.com', port='5432'
        )
        cursor = conn.cursor()
        schemaSet = "set schema 'tenant_details'"
        cursor.execute(schemaSet)
        query1 = "select * from tenant_details.test_tbl_user where county_name = '" + username.lower() + "'"
        cursor.execute(query1)
        record = cursor.fetchone()
        if record[len(record) - 1] == False:
            return Response({"message": "User Is Not Active It Will Active After few hours"},
                            status=status.HTTP_400_BAD_REQUEST)
        # print(record[3])
        Insertedpassword = DecodePassword(bytes(record[3], 'utf-8'))
        if Insertedpassword == password:
            query1 = "select * from test_tbl_county_details where unique_id = '" + record[0] + "'"
            cursor.execute(query1)
            record2 = cursor.fetchone()
            conn.commit()
            conn.close()
            accessTokenBI = generationAccesstoken(datasetId=record2[4], reportId=record2[3], groupId=record2[2])
            accessTokenML = generationAccesstoken(datasetId=record2[9], reportId=record2[12], groupId=record2[7])
            accessTokenMobileML = generationAccesstoken(datasetId=record2[9], reportId=record2[8], groupId=record2[7])
            MLToken=None
            MLTokenMobile=None
            try:
                MLToken = accessTokenML['token']
                MLTokenMobile = accessTokenMobileML['token']
            except Exception as e:
                print("Abhishek Test")
                print(e)
            # print("Abhishek")
            # print(accessTokenML)
            if len(record2) != 0:
                # print(len(record2))
                return Response(
                    {"username": username,
                     "unique_id": record2[0],

                    "groupIdML": record2[7],
                     "group_id": record2[2],

                     "report_id": record2[3],
                     "reportIdMLWeb": record2[12],
                     "reportIdMLMobile": record2[8],

                     "dataset_id": record2[4],
                     "datasetIdML": record2[9],

                     "BIiframe": record2[1],
                     "MLiframe_web": record2[5],
                     "MLIframeMobile": record2[13],

                     "accesstokenBI": accessTokenBI['token'],
                     "accesstokenML": MLToken,
                     "accesstokenMobileML": MLTokenMobile,

                     "embeded_url_web_BI": record2[6],
                     "embeddedUrlMobileML": record2[10],
                     "embeddedUrlWebML": record2[11]

                     }, status=status.HTTP_200_OK)
            else:
                return Response({"message": "Not getting proper records"}, status=status.HTTP_400_BAD_REQUEST)

        else:
            conn.commit()
            conn.close()
            return Response({"message": "Invalid username or password"}, status=status.HTTP_400_BAD_REQUEST)


def createTablesAndSchemaPostgres(name):
    conn = psycopg2.connect(
        database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com', port='5432'
    )
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    string = "SET SCHEMA '" + name + "'"
    cursor.execute("CREATE SCHEMA IF NOT EXISTS " + name + " ;")
    cursor.execute(string)
    table1 = '''
    CREATE TABLE IF NOT EXISTS "LRCMarketAreaStatistics"
(
    "DimMarketAreaStatisticsPk" bigint,
    "DimMarketAreaPk" bigint,
    "NEIGHBORHOOD_STATISTICS_PK" bigint,
    "NBM_NEIGHBORHOOD_MASTER_PK" character varying(20) COLLATE pg_catalog."default",
    "REAPPRAISAL_SEQ" numeric(4,0),
    "MEDIAN_GRADE" character varying(24) COLLATE pg_catalog."default",
    "MEDIAN_DEPR_RATING" character varying(24) COLLATE pg_catalog."default",
    "NOTES" character varying(200) COLLATE pg_catalog."default",
    "COMMON_AREA_PER_UNIT" numeric(12,2),
    "COMMON_AREA_PER_SQFT" numeric(12,2),
    "COMMON_AREA_PER_BLDG_AREA" numeric(10,0),
    "COMMON_AREA_YEAR_BUILT" numeric(4,0),
    "STANDARD_UNIT_SQ_FOOTAGE" numeric(12,0),
    "RETAIL_SQFT_LOW" numeric(12,0),
    "RETAIL_SQFT_HIGH" numeric(12,0),
    "OFFICE_SQFT_LOW" numeric(12,0),
    "OFFICE_SQFT_HIGH" numeric(12,0),
    "IND_SQFT_LOW" numeric(12,0),
    "IND_SQFT_HIGH" numeric(12,0),
    "APT_SQFT_LOW" numeric(12,0),
    "APT_SQFT_HIGH" numeric(12,0),
    "PURPOSE" character(1) COLLATE pg_catalog."default",
    "SQFT_RATE" numeric(12,2),
    "TOTAL_BUILDINGS" numeric(20,0),
    "TOTAL_PARCEL" numeric(20,0),
    "TOTAL_Q_SALES" numeric(20,0),
    "TOTAL_LAND_VALUE" numeric(20,0),
    "TOTAL_MEDIAN_EFFECTIVE_AGE" numeric(4,0),
    "MEDIAN_IMPROVED_VALUE" numeric(20,0),
    "MAX_TOTAL_VALUE" numeric(12,0),
    "MIN_TOTAL_VALUE" numeric(12,0),
    "MEAN_TOTAL_VALUE" numeric(12,0),
    "MEDIAN_TOTAL_VALUE" numeric(12,0),
    "PRICE_RELATED_DIFFERENTIAL" numeric(20,0),
    "COEFFICIENT_OF_DISPERSION" numeric(20,0),
    "MEDIAN_SQFT" numeric(12,0),
    "MEDIAN_ACREAGE" numeric(12,0),
    "MEDIAN_YEAR_BUILT" numeric(4,0),
    "SALES_PRICE" numeric(12,0),
    "PRICE_PER_SQFT" numeric(12,2),
    "GROSS_SQUARE_FEET" numeric(6,0),
    "GROSS_SQUARE_FEET_SALES" numeric(6,0),
    "STANDARD_DEVIATION" numeric(20,6),
    "AVERAGE_DEVIATION" numeric(20,6),
    "CONFIDENCE_INTERVAL_68" numeric(20,6),
    "CONFIDENCE_INTERVAL_95" numeric(20,6),
    "AGGREGATE_RATIO" numeric(20,6),
    "MEAN_SALE_RATIO" numeric(20,6),
    "MEDIAN_SALE_RATIO" numeric(20,6),
    "WEIGHTED_SALE_RATIO" numeric(20,6),
    "MEDIAN_BUILDING_VALUE" bigint,
    "MEDIAN_ECON_DEPR" integer,
    "MEDIAN_FUNC_DEPR" integer,
    "MEDIAN_EFFECTIVE_YEAR" integer,
    "MEDIAN_SFLA" integer,
    "MODE_YEAR_BUILT" character varying(4000) COLLATE pg_catalog."default",
    "MODE_DEPR_RATING" character varying(4000) COLLATE pg_catalog."default",
    "MODE_GRADE" character varying(4000) COLLATE pg_catalog."default",
    "MODE_NUM_STORIES" character varying(4000) COLLATE pg_catalog."default",
    "MEDIAN_LAND_VALUE" bigint,
    "MODE_LAND_DESCRIPTION" character varying(4000) COLLATE pg_catalog."default",
    "MODE_VALUE_APPROACH" character varying(4000) COLLATE pg_catalog."default",
    "REPEATED_BUILDING_COUNT_FOR_SALES" integer,
    "TOTAL_PARCEL_ASSESSED_VALUE" numeric(20,0),
    "TXN_CRUSER" numeric(10,0),
    "TXN_USER" numeric(10,0),
    "TXN_CRDATE" timestamp(3) without time zone,
    "TXN_DATE" timestamp(3) without time zone,
    "MARKET_AREA_ID" character varying(100) COLLATE pg_catalog."default",
    "MODULE_NAME" character varying(100) COLLATE pg_catalog."default"
)
    '''
    table2 = '''
    CREATE TABLE IF NOT EXISTS "LRCOwnership"
(
    "DimOwnershipOwnerPk" bigint NOT NULL,
    "DimOwnershipPk" bigint NOT NULL,
    "PAR_PARCEL_PK" bigint NOT NULL,
    "OWNERSHIP_PK" bigint NOT NULL,
    "OWNER_NAME" character varying(500) COLLATE pg_catalog."default",
    "OWNERSHIP_STATUS" character(100) COLLATE pg_catalog."default",
    "OWNERSHIP_HISTORY_SEQ" integer NOT NULL,
    "ERROR_FLAG" character(100) COLLATE pg_catalog."default" NOT NULL,
    "OWNERSHIP_BILLING_CLASS" character varying(240) COLLATE pg_catalog."default",
    "OWNERSHIP_BILLING_CLASS_DESC" character varying(200) COLLATE pg_catalog."default",
    "PARTIAL_INTEREST_SALE_FLAG" character(100) COLLATE pg_catalog."default",
    "OWNERSHIP_EXEMPT_CODE" character varying(240) COLLATE pg_catalog."default",
    "OWNERSHIP_EXEMPT_CODE_DESC" character varying(200) COLLATE pg_catalog."default",
    "HISTORY_CREATE_DATE" timestamp(3) without time zone,
    "HISTORY_CREATE_OPID" numeric(10,0),
    "TRANSFER_OWNERSHIP_FLAG" character(100) COLLATE pg_catalog."default",
    "OWNERSHIP_DEED_PK" bigint,
    "OWNERSHIP_DEED_DATE" timestamp(3) without time zone,
    "OWNERSHIP_DEED_BOOK" character varying(100) COLLATE pg_catalog."default",
    "OWNERSHIP_DEED_PAGE" character varying(100) COLLATE pg_catalog."default",
    "OWNERSHIP_DOCUMENT_NUMBER" character varying(100) COLLATE pg_catalog."default",
    "OWNERSHIP_DOCUMENT_TYPE" character varying(240) COLLATE pg_catalog."default",
    "OWNERSHIP_DOCUMENT_TYPE_DESC" character varying(200) COLLATE pg_catalog."default",
    "OWNERSHIP_REVENUE_STAMP" numeric(12,2),
    "OWNERSHIP_PRIMARY_DEED_FLAG" character(100) COLLATE pg_catalog."default",
    "ROD_DOC_ID" numeric(10,0),
    "DEED_BILL_YEAR" character varying(100) COLLATE pg_catalog."default",
    "BC_PROCESS_DATE" timestamp(3) without time zone,
    "DISPLAY_DEED_FLAG" character(100) COLLATE pg_catalog."default",
    "IS_JAN1_OWNERSHIP" character(100) COLLATE pg_catalog."default" NOT NULL,
    "OWNERSHIP_OWNER_PK" bigint,
    "NAM_NAME_PK" bigint,
    "ADD_ADDRESS_PK" bigint,
    "IDM_ID_PK" bigint,
    "PCT_OWNERSHIP" numeric(13,10),
    "NUMERATOR" numeric(10,0),
    "GROUP_TYPE" character varying(240) COLLATE pg_catalog."default",
    "GROUP_TYPE_DESC" character varying(200) COLLATE pg_catalog."default",
    "GROUP_NUM" numeric(10,0),
    "DENOMINATOR" numeric(10,0),
    "SEND_MAIL_FLAG" character varying(240) COLLATE pg_catalog."default",
    "SEND_MAIL_FLAG_DESC" character varying(200) COLLATE pg_catalog."default",
    "OWNER_ORDER" character varying(100) COLLATE pg_catalog."default",
    "ORIG_RDMOWN_SEQ_NUM" numeric(6,0),
    "NAME_TYPE" character(100) COLLATE pg_catalog."default",
    "LAST_NAME" character varying(35) COLLATE pg_catalog."default",
    "FIRST_NAME" character varying(25) COLLATE pg_catalog."default",
    "MIDDLE_NAME" character varying(25) COLLATE pg_catalog."default",
    "NAME_PREFIX" character varying(240) COLLATE pg_catalog."default",
    "NAME_SUFFIX" character varying(240) COLLATE pg_catalog."default",
    "ADDITIONAL_NAME" character varying(350) COLLATE pg_catalog."default",
    "BUSINESS_NAME" character varying(800) COLLATE pg_catalog."default",
    "EXTRA_NAME" character varying(800) COLLATE pg_catalog."default",
    "EXTRA_NAME_TYPE" character varying(240) COLLATE pg_catalog."default",
    "EXTRA_NAME_TYPE_DESC" character varying(200) COLLATE pg_catalog."default",
    "IN_CARE_OF" character varying(350) COLLATE pg_catalog."default",
    "OWNERSHIP_ADDRESS_TYPE" character(100) COLLATE pg_catalog."default",
    "OWNERSHIP_ADDRESS_TYPE_DESC" character(200) COLLATE pg_catalog."default",
    "MAILING_ADDRESS1" character varying(150) COLLATE pg_catalog."default" NOT NULL,
    "MAILING_ADDRESS2" character varying(150) COLLATE pg_catalog."default",
    "MAILING_ADDRESS3" character varying(150) COLLATE pg_catalog."default",
    "CITY" character varying(350) COLLATE pg_catalog."default",
    "POSTAL_CODE" character varying(100) COLLATE pg_catalog."default",
    "POSTAL_CODE_EXT" character varying(100) COLLATE pg_catalog."default",
    "COUNTRY_CODE" character varying(300) COLLATE pg_catalog."default",
    "BYPASS_FLAG" character(100) COLLATE pg_catalog."default",
    "EMAIL_ADDRESS" character varying(400) COLLATE pg_catalog."default",
    "OWNER_CATEGORY" character varying(120) COLLATE pg_catalog."default",
    "INCOME" numeric(10,2),
    "GENDER" character(100) COLLATE pg_catalog."default",
    "MARITAL_STATUS" character varying(240) COLLATE pg_catalog."default",
    "OWNER_PK" bigint,
    "OWNER_ID" character varying(100) COLLATE pg_catalog."default",
    "OWNER_TYPE" character varying(240) COLLATE pg_catalog."default",
    "OWNER_TYPE_DESC" character varying(200) COLLATE pg_catalog."default",
    "DRIVERS_LICENSE_NUM" character varying(150) COLLATE pg_catalog."default",
    "DATE_OF_BIRTH" timestamp(3) without time zone,
    "OWNER_SUB_TYPE" character varying(240) COLLATE pg_catalog."default",
    "OWNER_SUB_TYPE_DESC" character varying(200) COLLATE pg_catalog."default",
    "TXN_CRUSER" numeric(10,0),
    "TXN_USER" numeric(10,0),
    "TXN_CRDATE" timestamp(3) without time zone,
    "TXN_DATE" timestamp(3) without time zone,
    "STATE" character varying(100) COLLATE pg_catalog."default",
    "MODULE_NAME" character varying(100) COLLATE pg_catalog."default",
    "OWNER_ADDRESS" character varying(500) COLLATE pg_catalog."default"
)
    '''
    table3 = '''
    CREATE TABLE IF NOT EXISTS "LRCParcel"
(
    "DimParcelPk" bigint,
    "DimMarketAreaPk" bigint,
    "DimPhysicalAddressPk" bigint,
    "DimOwnershipPk" bigint,
    "DimSitusPk" bigint,
    "PAR_PARCEL_PK" bigint,
    "AUT_SNAPSHOT_DATE" timestamp(3) without time zone,
    "MARKET_AREA_ID" character varying(100) COLLATE pg_catalog."default",
    "PARCEL_NUM" character varying(200) COLLATE pg_catalog."default",
    "REAPPRAISAL_SEQ" numeric(9,0),
    "RECORD_TYPE" character varying(240) COLLATE pg_catalog."default",
    "RECORD_TYPE_DESC" character varying(200) COLLATE pg_catalog."default",
    "RECORD_STATUS" character(100) COLLATE pg_catalog."default",
    "RECORD_ORDER" integer,
    "PARCEL_STATUS" character varying(240) COLLATE pg_catalog."default",
    "PARCEL_STATUS_DESC" character varying(200) COLLATE pg_catalog."default",
    "RETIRED_DATE" timestamp(0) without time zone,
    "MILLI_SECONDS" integer,
    "SPECIAL_DESC" character varying(255) COLLATE pg_catalog."default",
    "VALUE_APPROACH" character varying(240) COLLATE pg_catalog."default",
    "VALUE_APPROACH_DESC" character varying(200) COLLATE pg_catalog."default",
    "ACCOUNT_TYPE" character varying(240) COLLATE pg_catalog."default",
    "ACCOUNT_TYPE_DESC" character varying(200) COLLATE pg_catalog."default",
    "HAS_LAND_USE_VALUE" character varying(100) COLLATE pg_catalog."default",
    "LAND_MARKET_VALUE" numeric(12,0),
    "PARCEL_COST_VALUE" numeric(12,0),
    "PARCEL_INCOME_VALUE" numeric(12,0),
    "PARCEL_SALES_COMP_VALUE" numeric(12,0),
    "PARCEL_TOTAL_VALUE" numeric(12,0),
    "PARCEL_TOTAL_DEF_EXEMPT_VALUE" numeric(12,0),
    "PARCEL_TOTAL_TAXABLE_VALUE" numeric(12,0),
    "PARCEL_TOTAL_LAND_DEF_VALUE" numeric(12,0),
    "PARCEL_TOTAL_HIST_DEF_VALUE" numeric(12,0),
    "PARCEL_TOTAL_EXEMPT_VALUE" numeric(12,0),
    "PARCEL_USE_VALUE" numeric(12,0),
    "PARCEL_USE_VALUE_YEAR" numeric(9,0),
    "DEEDED_ACREAGE" numeric(10,2),
    "CALCULATED_ACREAGE" numeric(8,2),
    "PIN_NUM" character varying(14) COLLATE pg_catalog."default",
    "PIN_EXT" character varying(3) COLLATE pg_catalog."default",
    "PIN_MAP" character varying(100) COLLATE pg_catalog."default",
    "PIN_BLOCK" character varying(200) COLLATE pg_catalog."default",
    "PIN_LOT" character varying(100) COLLATE pg_catalog."default",
    "PROPERTY_DESCRIPTION" character varying(200) COLLATE pg_catalog."default",
    "PARCEL_LAND_VALUE_ASSD" numeric(12,0),
    "PARCEL_LAND_VALUE_APPR" numeric(12,0),
    "PARCEL_MISC_IMPRV_VALUE_ASSD" numeric(9,0),
    "PARCEL_MISC_IMPRV_VALUE_APPR" numeric(9,0),
    "PARCEL_BLDG_VALUE_ASSD" numeric(12,0),
    "PARCEL_BLDG_VALUE_APPR" numeric(12,0),
    "TOTAL_BLDG_VALUE_ASSD" numeric(12,0),
    "TOTAL_BLDG_VALUE_APPR" numeric(12,0),
    "TOTAL_LAND_UNITS" numeric(12,2),
    "TOTAL_USE_VALUE" numeric(12,0),
    "TOTAL_DEF_VALUE" numeric(12,0),
    "TOTAL_VALUE_SQFT" numeric(15,6),
    "TOTAL_VALUE_ACRE" numeric(15,6),
    "TOTAL_VALUE_UNIT" numeric(15,6),
    "MISC_IMPRV_COUNT" numeric(9,0),
    "LANDLINE_COUNT" numeric(9,0),
    "TOT_RECYCLE_UNIT" numeric(9,0),
    "SUBDIVISION" character varying(240) COLLATE pg_catalog."default",
    "SUBDIVISION_DESC" character varying(200) COLLATE pg_catalog."default",
    "USE_VALUE_DEFERRED_AMT" numeric(12,0),
    "TAX_RELIEF_AMT" numeric(12,0),
    "HIST_DEFERRED_AMT" numeric(12,0),
    "TOT_ADJUSTMENT_VALUE" numeric(12,0),
    "SUBMARKET" character varying(240) COLLATE pg_catalog."default",
    "LOT_NUMBER" character varying(100) COLLATE pg_catalog."default",
    "SUBDIVISION_DISP" character varying(150) COLLATE pg_catalog."default",
    "TRACT" character varying(100) COLLATE pg_catalog."default",
    "PHASE" character varying(100) COLLATE pg_catalog."default",
    "SECTION" character varying(100) COLLATE pg_catalog."default",
    "BLOCK" character varying(100) COLLATE pg_catalog."default",
    "BLDG_NUMBER" character varying(100) COLLATE pg_catalog."default",
    "UNIT_NUMBER" character varying(100) COLLATE pg_catalog."default",
    "STATE_ASSESSED_FLAG" character(100) COLLATE pg_catalog."default",
    "HOMEOWNERS_ASSOC_FLAG" character(100) COLLATE pg_catalog."default",
    "MAP_BOOK" character varying(100) COLLATE pg_catalog."default",
    "MAP_PAGE" character varying(100) COLLATE pg_catalog."default",
    "MAP_SCALE" character varying(200) COLLATE pg_catalog."default",
    "MAP_ROD_DOC_ID" numeric(9,0),
    "PLAT_BOOK" character varying(100) COLLATE pg_catalog."default",
    "PLAT_PAGE" character varying(100) COLLATE pg_catalog."default",
    "ASSESSED_ACREAGE" numeric(8,2),
    "PARCELFLAG" character varying(200) COLLATE pg_catalog."default",
    "TOTAL_HEATED_AREA" numeric(12,0),
    "LAND_VALUE_RATIO" numeric(9,2),
    "TOTAL_UNITS" numeric(9,0),
    "ORIG_BLDG_NUM" numeric(7,0),
    "LAND_MOD_DATE" timestamp(3) without time zone,
    "REVIEWED_BY_APPRAISER" character varying(10) COLLATE pg_catalog."default",
    "VAL_CHANGE_APPRAISER" character varying(10) COLLATE pg_catalog."default",
    "VAL_CHANGE_BLDG_VALUE" numeric(11,0),
    "MASS_UPDATE_DATE" timestamp(3) without time zone,
    "MASS_UPDATE_OPID" numeric(10,0),
    "OVERRIDE_FINAL_VALUE" character(100) COLLATE pg_catalog."default",
    "DEED_NOTES" character varying(2000) COLLATE pg_catalog."default",
    "PLAT_BUILDING" character varying(100) COLLATE pg_catalog."default",
    "PLAT_LOT" character varying(100) COLLATE pg_catalog."default",
    "PARCEL_ID" character varying(200) COLLATE pg_catalog."default",
    "FARM_USE_YEAR" numeric(9,0),
    "TOBACCO_LBS" numeric(7,0),
    "ACREAGE_IS_CALC_FLAG" character(100) COLLATE pg_catalog."default",
    "TOBACCO_VAL_APPR" numeric(10,0),
    "TOBACCO_VAL_ASSD" numeric(10,0),
    "TOTAL_PROP_VALUE_APPR" numeric(12,0),
    "BLDG_USE" character varying(24) COLLATE pg_catalog."default",
    "BLDG_TYPE_USE_DISP_STR" character varying(50) COLLATE pg_catalog."default",
    "TAX_RELIEF_FLAG" character(100) COLLATE pg_catalog."default",
    "VETERANS_FLAG" character(100) COLLATE pg_catalog."default",
    "VETERANS_EXCL_AMT" numeric(12,0),
    "RETAINUSEVALUEFORPARCELPK" numeric(10,0),
    "CROSS_REFERENCE_BOOK" character varying(101) COLLATE pg_catalog."default",
    "CROSS_REFERENCE_PAGE" character varying(100) COLLATE pg_catalog."default",
    "HISTORICAL_PLAT_BOOK" character varying(100) COLLATE pg_catalog."default",
    "HISTORICAL_PLAT_PAGE" character varying(100) COLLATE pg_catalog."default",
    "PLAT_NOTES" character varying(2000) COLLATE pg_catalog."default",
    "ESTATE_FILE_NUM" character varying(100) COLLATE pg_catalog."default",
    "CENSUS_TRACT" character varying(100) COLLATE pg_catalog."default",
    "X_COORDINATE" numeric(15,5),
    "Y_COORDINATE" numeric(15,5),
    "MAP_NUMBER_4X6" character varying(150) COLLATE pg_catalog."default",
    "PIN_MAP_200_SCALE" character varying(150) COLLATE pg_catalog."default",
    "NET_LEASABLE_AREA" bigint,
    "TOTAL_GLA" numeric(12,0),
    "TOTAL_SFLA" numeric(12,0),
    "PARCEL_LAND_ZONING" character varying(150) COLLATE pg_catalog."default",
    "LEGACY_PARCEL_ID" character varying(150) COLLATE pg_catalog."default",
    "MODULE_NAME" character varying(150) COLLATE pg_catalog."default",
    "TXN_CRUSER" numeric(10,0),
    "TXN_USER" numeric(10,0),
    "TXN_CRDATE" timestamp(3) without time zone,
    "TXN_DATE" timestamp(3) without time zone
)
    '''
    table4 = '''
    CREATE TABLE IF NOT EXISTS "LRCPermit"
(
    "DimPermitPk" bigint,
    "DimParcelPk" bigint,
    "PERMIT_PK" bigint,
    "PERMIT_ZONING" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_TOWNSHIP" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_TOWNSHIP_DESC" character varying(28) COLLATE pg_catalog."default",
    "PERMIT_SUITE_OR_UNIT" character varying(10) COLLATE pg_catalog."default",
    "PERMIT_SUBDIVISION" character varying(100) COLLATE pg_catalog."default",
    "PERMIT_PLAT_YEAR" character varying(4) COLLATE pg_catalog."default",
    "PERMIT_PLAT_PAGE" character varying(8) COLLATE pg_catalog."default",
    "PERMIT_PLANNING_JURISDICTION" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_PLANNING_JURISDICTION_DESC" character varying(20) COLLATE pg_catalog."default",
    "PERMIT_TYPE" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_TYPE_DESC" character varying(20) COLLATE pg_catalog."default",
    "PERMIT_STATUS" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_STATUS_DESC" character varying(20) COLLATE pg_catalog."default",
    "PERMIT_PIN" character varying(14) COLLATE pg_catalog."default",
    "PERMIT_PARCEL_NUM" character varying(20) COLLATE pg_catalog."default",
    "PERMIT_NUMBER" character varying(14) COLLATE pg_catalog."default",
    "PERMIT_ISSUE_DATE" timestamp(3) without time zone,
    "PERMIT_ADDRESS" character varying(100) COLLATE pg_catalog."default",
    "PERMIT_OWNER_NAME" character varying(100) COLLATE pg_catalog."default",
    "PERMIT_OCCUPANT_NAME" character varying(100) COLLATE pg_catalog."default",
    "PERMIT_NOTES" character varying(1000) COLLATE pg_catalog."default",
    "PERMIT_MANUAL_FLAG" character(1) COLLATE pg_catalog."default",
    "PERMIT_INSPECTION_COMPLETION_DATE" timestamp(3) without time zone,
    "PERMIT_INSIDE_CORPORATE_LIMIT" character(1) COLLATE pg_catalog."default",
    "PERMIT_DIRECTIONS" character varying(200) COLLATE pg_catalog."default",
    "PERMIT_DEPARTMENT_TYPE" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_COUNTY_TYPE_USE" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_CERTIFICATE_OCCUPIED_BY" character varying(100) COLLATE pg_catalog."default",
    "PERMIT_CERTIFICATE_OCCUPANCY_DATE" timestamp(3) without time zone,
    "PERMIT_BUSINESS_NAME" character varying(100) COLLATE pg_catalog."default",
    "PERMIT_APPLICANT_NAME" character varying(80) COLLATE pg_catalog."default",
    "PERMIT_ACRES" numeric(8,2),
    "PERMIT_YEAR_FOR" numeric(4,0),
    "PERMIT_VALUATION_IMPACT_COMPLETE_BY" numeric(10,0),
    "PERMIT_VALUATION_IMPACT" character varying(24) COLLATE pg_catalog."default",
    "PERMIT_VALUATION_IMPACT_DESC" character varying(20) COLLATE pg_catalog."default",
    "PERMIT_URL" text COLLATE pg_catalog."default",
    "PERMIT_VOID_DATE" timestamp(3) without time zone,
    "TXN_CRUSER" numeric(10,0),
    "TXN_USER" numeric(10,0),
    "TXN_CRDATE" timestamp(3) without time zone,
    "TXN_DATE" timestamp(3) without time zone,
    "MODULE_NAME" character varying(100) COLLATE pg_catalog."default",
    "PERMIT_LOT_NUM" character varying(10) COLLATE pg_catalog."default",
    "PERMIT_PROJECT_NUMBER" character varying(40) COLLATE pg_catalog."default"
)
    '''

    table5 = '''
    CREATE TABLE IF NOT EXISTS "LRCPhysicalAddress"
(
    "DimPhysicalAddressPk" bigint,
    "DimParcelPk" bigint,
    "DimResidentialBuildingPk" bigint,
    "DimSectionBuildingPk" bigint,
    "CARD_NUM" numeric(3,0),
    "BUILDING_TYPE" character(1) COLLATE pg_catalog."default",
    "PAL_PK" bigint,
    "PAR_PARCEL_PK" bigint,
    "BDL_BUILDING_PK" bigint,
    "AUT_SNAPSHOT_DATE" timestamp(0) without time zone,
    "PRIMARY_ADDRESS_FLAG" character(1) COLLATE pg_catalog."default",
    "PHYSICAL_ADDRESS_PK" bigint,
    "PHYSICAL_ADDRESS_TYPE" character varying(24) COLLATE pg_catalog."default",
    "PHYSICAL_ADDRESS_TYPE_DESC" character varying(20) COLLATE pg_catalog."default",
    "CORNER_LOT_FLAG" character(1) COLLATE pg_catalog."default",
    "STREET_NUM" numeric(6,0),
    "STREET_MISC" character varying(6) COLLATE pg_catalog."default",
    "STATUS" character varying(24) COLLATE pg_catalog."default",
    "STATUS_DESC" character varying(20) COLLATE pg_catalog."default",
    "POSTAL_CITY" character varying(35) COLLATE pg_catalog."default",
    "POSTAL_CITY_DESC" character varying(20) COLLATE pg_catalog."default",
    "ZIP" character varying(5) COLLATE pg_catalog."default",
    "ZIP_PLUS" character varying(4) COLLATE pg_catalog."default",
    "CARRIER_ROUTE" character varying(4) COLLATE pg_catalog."default",
    "CODE1_APT_IN" character varying(4) COLLATE pg_catalog."default",
    "CODE1_APT_CODE" character varying(8) COLLATE pg_catalog."default",
    "NO_ADDRESS" character(1) COLLATE pg_catalog."default",
    "NO_ADDRESS_TXN_DATE" timestamp(0) without time zone,
    "STR_LOCID" bigint,
    "STREET_NAME" character varying(30) COLLATE pg_catalog."default",
    "DIRECTIONAL_PREFIX" character varying(24) COLLATE pg_catalog."default",
    "DIRECTIONAL_PREFIX_DESC" character varying(20) COLLATE pg_catalog."default",
    "DIRECTIONAL_SUFFIX" character varying(24) COLLATE pg_catalog."default",
    "DIRECTIONAL_SUFFIX_DESC" character varying(20) COLLATE pg_catalog."default",
    "STREET_TYPE" character varying(24) COLLATE pg_catalog."default",
    "STREET_TYPE_DESC" character varying(20) COLLATE pg_catalog."default",
    "UNIQUE_SEQ_NUM" numeric(1,0),
    "PLAN_JURIS" character varying(24) COLLATE pg_catalog."default",
    "PLAN_JURIS_DESC" character varying(20) COLLATE pg_catalog."default",
    "STREET_STATUS" character varying(24) COLLATE pg_catalog."default",
    "STREET_STATUS_DESC" character varying(20) COLLATE pg_catalog."default",
    "DATE_STREET_ENTERED" timestamp(0) without time zone,
    "NOTES" character varying(80) COLLATE pg_catalog."default",
    "APPLICATION_NUM" numeric(5,0),
    "SUBDIVISION_NAME" character varying(30) COLLATE pg_catalog."default",
    "DATE_APPROVED" timestamp(0) without time zone,
    "APPROVED_BY_OPID" character varying(3) COLLATE pg_catalog."default",
    "ACTUAL_MIN_STREET_NUM" numeric(6,0),
    "ACTUAL_MAX_STREET_NUM" numeric(6,0),
    "PRIOR_NAME_LOCID" numeric(6,0),
    "ZIP_CODE" character varying(5) COLLATE pg_catalog."default",
    "ZIP_EXTENSION" character varying(4) COLLATE pg_catalog."default",
    "NUM_OF_BLDGS" numeric(7,0),
    "ALLOWED_MIN_STREET_NUM" numeric(6,0),
    "ALLOWED_MAX_STREET_NUM" numeric(6,0),
    "INITIALS" character varying(3) COLLATE pg_catalog."default",
    "ADDRESS_ORDER" integer,
    "PRIMARY_ADDRESS_ORDER" integer,
    "MODULE_NAME" character varying(50) COLLATE pg_catalog."default",
    "TXN_CRUSER" numeric(10,0),
    "TXN_USER" numeric(10,0),
    "TXN_CRDATE" timestamp(0) without time zone,
    "TXN_DATE" timestamp(0) without time zone
)
    '''

    table6 = ''' CREATE TABLE IF NOT EXISTS "LRCResidentialBuilding"
(
    "DimResidentialBuildingPk" bigint,
    "DimParcelPk" bigint,
    "DimPhysicalAddressPk" bigint,
    "RES_BLDG_HAS_OVERRIDES" character(100) COLLATE pg_catalog."default",
    "RES_BUILDING_NUM" numeric(10,0),
    "RES_BLDG_LISTER" character varying(300) COLLATE pg_catalog."default",
    "RES_BLDG_LISTER_DATE" timestamp(0) without time zone,
    "RES_BLDG_REFERENCE_FLAG" character(100) COLLATE pg_catalog."default",
    "RES_BLDG_SPECIAL_DESC" character varying(200) COLLATE pg_catalog."default",
    "RES_BLDG_SPECIAL_DESC_RATE" numeric(10,2),
    "RES_BLDG_INCOMPLETE_STRUCT_VALUE" numeric(15,0),
    "RES_BLDG_INCOMPLETE_STRUCT_PCT" numeric(9,0),
    "RES_BLDG_YEAR_BUILT" numeric(9,0),
    "RES_BLDG_NUM_LIVING_UNITS" numeric(9,0),
    "RES_BLDG_REVIEW_REQD_FLAG" character(100) COLLATE pg_catalog."default",
    "RES_BLDG_REVIEW_REQD_DATE" timestamp(0) without time zone,
    "RES_BLDG_REVIEW_NOTES" character varying(500) COLLATE pg_catalog."default",
    "RES_BLDG_REVIEW_YEAR_FOR" numeric(15,0),
    "RES_BLDG_BASE_SCHEDULE_RATE" numeric(15,6),
    "RES_BLDG_ADD_DEDUCTS_RATE" numeric(15,6),
    "RES_BLDG_EFFECTIVE_YEAR" numeric(9,0),
    "RES_BLDG_RECYCLE_UNIT" numeric(9,0),
    "RES_BLDG_ACCRUED_MKT_ADJ_PCT" numeric(15,6),
    "RES_BLDG_ECONOMIC_LIFE" numeric(10,0),
    "RES_BLDG_REPLACEMENT_VALUE" numeric(9,0),
    "RES_BLDG_ADJ_REPLACEMENT_VALUE" numeric(9,0),
    "RES_BLDG_APPR_VALUE" numeric(9,0),
    "RES_BLDG_ASSD_VALUE" numeric(9,0),
    "RES_BLDG_MISC_IMPRV_APPR_VALUE" numeric(9,0),
    "RES_BLDG_MISC_IMPRV_ASSD_VALUE" numeric(9,0),
    "TOTAL_RES_BLDG_APPR_VALUE" numeric(9,0),
    "TOTAL_RES_BLDG_ASSD_VALUE" numeric(9,0),
    "RES_BLDG_ACTUAL_FIREPLACE_NUM" numeric(9,0),
    "RES_BLDG_TOTAL_LIVING_AREA" numeric(9,0),
    "RES_BLDG_SCHEDULE_VALUE_TOTAL" numeric(9,0),
    "RES_BLDG_ACCRUED_PERCENT" numeric(15,6),
    "RES_BLDG_BATH_FULL" numeric(10,0),
    "RES_BLDG_BATH_HALF" numeric(10,0),
    "RES_BLDG_EXTRA_FIXTURES" numeric(10,0),
    "RES_BLDG_TOTAL_FIXTURES" numeric(10,0),
    "RES_BLDG_BATH_TOTAL" numeric(11,2),
    "RES_BLDG_UNLIVABLE_FLAG" character(100) COLLATE pg_catalog."default",
    "RES_BLDG_PERCENT_COMPLETE" numeric(9,0),
    "RES_BLDG_FOOTPRINT_AREA" numeric(9,0),
    "RES_BLDG_SKETCH_VECTOR" character varying(300) COLLATE pg_catalog."default",
    "RES_BLDG_IS_SKETCHED" character(100) COLLATE pg_catalog."default",
    "RES_BLDG_SIZE_EQUIVALENT_FLAG" character(100) COLLATE pg_catalog."default",
    "RES_BLDG_ATTIC_FINISH_SF" numeric(9,0),
    "RES_BLDG_BASEMENT_FINISH_SF" numeric(9,0),
    "RES_BLDG_UNFINISHED_INT_SF" numeric(9,0),
    "RES_BLDG_SCHEDULE_RATE_ADJ" numeric(9,2),
    "RES_BLDG_VALUE" numeric(11,2),
    "RES_BLDG_PERIMETER" numeric(9,0),
    "RES_BLDG_WALL_RATIO" numeric(9,2),
    "RES_BLDG_NUM_STORIES" numeric(5,2),
    "RES_BLDG_NUM_STORIES_MULTIPLIER" numeric(10,6),
    "RES_BLDG_BUILT_IN_TOTAL_RATE" numeric(15,6),
    "RES_BLDG_PRIMARY_FLAG" character(100) COLLATE pg_catalog."default",
    "RES_BLDG_HIST_DEFERRED_PCT" numeric(9,0),
    "RES_BLDG_LEGACY_ADD_DEDUCT_TOTAL_ASSD" numeric(7,2),
    "RES_BLDG_REMODELED_YR" numeric(9,0),
    "RES_BLDG_ACCR_COND_PERCENT_ASSD" numeric(9,0),
    "RES_BLDG_ADDTN_YR" numeric(9,0),
    "RES_BLDG_BLDG_VALUE_PREV_ASSD" numeric(11,0),
    "RES_BLDG_LISR_MOD_DATE" timestamp(0) without time zone,
    "RES_BLDG_SKETCH_MOD_DATE" timestamp(0) without time zone,
    "RES_BLDG_SKETCH_MOD_USER" character varying(10) COLLATE pg_catalog."default",
    "RES_BLDG_PREV_BLDG_VALUE" numeric(11,0),
    "RES_BLDG_PREV_OBLDG_VALUE" numeric(11,0),
    "RES_BLDG_BEDROOMS" numeric(10,0),
    "RES_BLDG_APEX_SKETCH_FILEPATH" character varying(100) COLLATE pg_catalog."default",
    "RES_BLDG_APEX_IMAGE_FILEPATH" character varying(100) COLLATE pg_catalog."default",
    "RES_BLDG_SIZE_FACTOR" numeric(7,5),
    "RES_BLDG_BUILDING_NAME" character varying(556) COLLATE pg_catalog."default",
    "RES_BLDG_MB_WALL_HEIGHT" numeric(9,2),
    "RES_BLDG_MB_WALL_HEIGHT_FACTOR" numeric(10,4),
    "RES_BLDG_ECONOMIC_DEPRECIATION_PCT" numeric(9,0),
    "RES_BLDG_ECONOMIC_DEPRECIATION_NOTE" character varying(200) COLLATE pg_catalog."default",
    "RES_BLDG_FUNCTIONAL_DEPRECIATION_PCT" numeric(9,0),
    "RES_BLDG_FUNCTIONAL_DEPRECIATION_NOTE" character varying(200) COLLATE pg_catalog."default",
    "RES_BLDG_REFINEMENT_TOTAL" numeric(11,2),
    "RES_BLDG_DEPRECIATED_VALUE" numeric(11,0),
    "RES_BLDG_TOTAL_DEPRECIATED_VALUE" numeric(11,0),
    "RES_BLDG_TOTAL_REPL_VALUE" numeric(11,0),
    "RES_BLDG_TOTAL_ADJ_REPL_VALUE" numeric(11,0),
    "RES_BLDG_FINAL_VALUE_OVRR" numeric(11,0),
    "RES_BLDG_UNCOMPENSATED_VALUE" numeric(12,0),
    "RES_BLDG_COMPENSATION_REASON" character varying(500) COLLATE pg_catalog."default",
    "RES_BLDG_REMODELING_YEAR" numeric(9,0),
    "RES_BLDG_REMODELING_DESCRIPTION" character varying(250) COLLATE pg_catalog."default",
    "RES_BLDG_REMODELING_ADDEDBY" character varying(300) COLLATE pg_catalog."default",
    "RES_BLDG_REMODELING_ADDEDDATE" timestamp(3) without time zone,
    "MODULE_NAME" character varying(50) COLLATE pg_catalog."default",
    "TXN_CRUSER" numeric(10,0),
    "TXN_USER" numeric(10,0),
    "TXN_CRDATE" timestamp(3) without time zone,
    "TXN_DATE" timestamp(3) without time zone,
    "RES_BLDG_CARD_NUMBER" numeric(9,0)
)

    '''

    table7 = '''CREATE TABLE IF NOT EXISTS "LRCResidentialBuildingFeature"
(
    "DimResidentialBuildingFeaturePk" bigint,
    "DimResidentialBuildingPk" bigint,
    "DimParcelPk" bigint,
    "RES_BLDG_FEATURE_SEQ_NUM" integer,
    "RES_BLDG_TYPE_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_USE" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_USE_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_DESIGN_STYLE" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_DESIGN_STYLE_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_DESIGN_STYLE_PERCENT" numeric(12,6),
    "RES_BLDG_ROOF_COVER" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_ROOF_COVER_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_ROOF_COVER_PERCENT" numeric(12,6),
    "RES_BLDG_ROOF_TYPE" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_ROOF_TYPE_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_ROOF_TYPE_PERCENT" numeric(12,6),
    "RES_BLDG_FRAME" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_FRAME_DESC" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_FRAME_PERCENT" numeric(12,6),
    "RES_BLDG_AIR_CONDITIONING" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_AIR_CONDITIONING_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_AIR_CONDITIONING_PERCENT" numeric(12,6),
    "RES_BLDG_EXTERIOR_WALL" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_EXTERIOR_WALL_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_EXTERIOR_WALL_PERCENT" numeric(12,6),
    "RES_BLDG_EXTERIOR_WALL_SEQ_NUM" integer,
    "RES_BLDG_FOUNDATION" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_FOUNDATION_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_FOUNDATION_PERCENT" numeric(12,6),
    "RES_BLDG_HEATING" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_HEATING_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_HEATING_PERCENT" numeric(12,6),
    "RES_BLDG_INTERIOR_FINISH" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_INTERIOR_FINISH_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_INTERIOR_FINISH_PERCENT" numeric(12,6),
    "RES_BLDG_INTERIOR_FINISH_SEQ_NUM" integer,
    "RES_BLDG_FIREPLACE" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_FIREPLACE_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_FIREPLACE_NUM_OCCURS1" numeric(10,2),
    "RES_BLDG_FIREPLACE_NUM_OCCURS2" numeric(10,2),
    "RES_BLDG_FIREPLACE_SEQ_NUM" integer,
    "RES_BLDG_FLOOR" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_FLOOR_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_FLOOR_PERCENT" numeric(12,6),
    "RES_BLDG_FLOOR_SEQ_NUM" integer,
    "RES_BLDG_DEPRECIATION_RATING" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_DEPRECIATION_RATING_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_DEPRECIATION_PERCENT" numeric(12,6),
    "RES_BLDG_GRADE" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_GRADE_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_GRADE_PERCENT" numeric(12,6),
    "RES_BLDG_ROOF_STRUCTURE" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_ROOF_STRUCTURE_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_ROOF_STRUCTURE_PERCENT" numeric(12,6),
    "RES_BLDG_MEZZANINE_FINISH" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_MEZZANINE_FINISH_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_MEZZANINE_PERCENT" numeric(12,6),
    "RES_BLDG_BUILT_IN" character varying(24) COLLATE pg_catalog."default",
    "RES_BLDG_BUILT_IN_DESC" character varying(20) COLLATE pg_catalog."default",
    "RES_BLDG_BUILT_IN_PERCENT" numeric(10,6),
    "RES_BLDG_BUILT_IN_NUM_OCCURS1" numeric(10,2),
    "RES_BLDG_BUILT_IN_NUM_OCCURS2" numeric(10,2),
    "RES_BLDG_BUILT_IN_VALUE" numeric(12,2),
    "RES_BLDG_BUILT_SEQ_NUM" integer,
    "MODULE_NAME" character varying(50) COLLATE pg_catalog."default",
    "TXN_DATE" timestamp(3) without time zone,
    "RES_BLDG_TYPE" character varying(100) COLLATE pg_catalog."default"
)

    '''

    table8 = '''
    CREATE TABLE IF NOT EXISTS "LRCSale"
(
    "DimSalePk" bigint,
    "DimMarketAreaPk" bigint,
    "DimParcelPk" bigint,
    "DimResidentialBuildingPk" bigint,
    "DimSectionBuildingPk" bigint,
    "DimPhysicalAddressPk" bigint,
    "PAR_PARCEL_PK" bigint,
    "SALE_PK" bigint,
    "SALE_DATE" timestamp(3) without time zone,
    "AUT_SNAPSHOT_DATE" timestamp(3) without time zone,
    "GRANTEE" character varying(255) COLLATE pg_catalog."default",
    "GRANTOR" character varying(255) COLLATE pg_catalog."default",
    "TOTAL_PROP_VALUE" numeric(18,0),
    "SALE_PRICE" numeric(18,2),
    "SALES_RATIO" numeric(20,6),
    "TDF_SALE_PRICE" numeric(18,2),
    "TDF_SALES_RATIO" numeric(20,6),
    "TDF_FACTORS" character varying(1000) COLLATE pg_catalog."default",
    "TOTAL_ACREAGE" numeric(10,2),
    "PRICE_PER_ACRE" numeric(16,2),
    "SALE_DEED_BOOK" character varying(6) COLLATE pg_catalog."default",
    "SALE_DEED_PAGE" character varying(5) COLLATE pg_catalog."default",
    "SALE_DOCUMENT_NUMBER" character varying(10) COLLATE pg_catalog."default",
    "SALE_REVENUE_STAMP_VALUE" numeric(12,2),
    "REVIEWED_BY" numeric(12,2),
    "SALE_DETAIL_PK" numeric(12,2),
    "SALE_TYPE" character varying(24) COLLATE pg_catalog."default",
    "SALE_TYPE_DESC" character varying(20) COLLATE pg_catalog."default",
    "SALE_STATUS" character varying(24) COLLATE pg_catalog."default",
    "SALE_STATUS_DESC" character varying(20) COLLATE pg_catalog."default",
    "SALE_DISQUALIFICATION_REASON" character varying(24) COLLATE pg_catalog."default",
    "SALE_DISQUALIFICATION_REASON_DESC" character varying(20) COLLATE pg_catalog."default",
    "DOR_EDIT_CODE" character varying(24) COLLATE pg_catalog."default",
    "DOR_EDIT_DESC" character varying(20) COLLATE pg_catalog."default",
    "SALE_PRICE_MLS" numeric(16,2),
    "SALE_PRICE_VERIFICATIONLETTER" numeric(16,2),
    "SALE_PRICE_OTHER" numeric(16,2),
    "SALE_PRICE_SOURCE" character varying(4) COLLATE pg_catalog."default",
    "SALE_PRICE_REVENUESTAMP" numeric(16,2),
    "SALE_SCCOMP" character(1) COLLATE pg_catalog."default",
    "HISTORY_LOAD_DATE" timestamp(3) without time zone,
    "SALE_DETAIL_ID" character varying(10) COLLATE pg_catalog."default",
    "VERIFICATION_LETTER_PRINT_DATE" timestamp(3) without time zone,
    "VERIFICATION_LETTER_PRINT_OPID" numeric(10,0),
    "IS_SALE_VERIFIED" character(1) COLLATE pg_catalog."default",
    "MILLI_SECONDS" integer,
    "SALE_YEAR_FOR" numeric(4,0),
    "MODULE_NAME" character varying(50) COLLATE pg_catalog."default",
    "TXN_CRUSER" numeric(10,0),
    "TXN_USER" numeric(10,0),
    "TXN_CRDATE" timestamp(3) without time zone,
    "TXN_DATE" timestamp(3) without time zone
)
    '''

    table9 = '''
    CREATE TABLE IF NOT EXISTS "LRCSitusTaxDistrict"
(
    "DimSitusTaxDistrictPk" bigint,
    "DimSitusPk" bigint,
    "PAR_PARCEL_PK" bigint,
    "SITUS_PK" bigint,
    "ANNEX_DATE" timestamp(3) without time zone,
    "ANNEX_CONTROL_NUM" character varying(24) COLLATE pg_catalog."default",
    "PRIOR_FIRE" character varying(24) COLLATE pg_catalog."default",
    "PRIOR_FIRE_DESC" character varying(20) COLLATE pg_catalog."default",
    "SCHOOL_DISTRICT" character varying(24) COLLATE pg_catalog."default",
    "SCHOOL_DISTRICT_DESC" character varying(20) COLLATE pg_catalog."default",
    "EMS_DISTRICT" character varying(24) COLLATE pg_catalog."default",
    "EMS_DISTRICT_DESC" character varying(20) COLLATE pg_catalog."default",
    "TOWNSHIP" character varying(24) COLLATE pg_catalog."default",
    "TOWNSHIP_DESC" character varying(20) COLLATE pg_catalog."default",
    "TAX_DISTRICT" character varying(24) COLLATE pg_catalog."default",
    "TAX_DISTRICT_DESC" character varying(20) COLLATE pg_catalog."default",
    "JURISDICTION_DATA" character varying(2000) COLLATE pg_catalog."default",
    "SITUS_SEQ_NUM" integer,
    "ANNEX_DATE_SEQ" integer,
    "COUNTY_CODE" character varying(24) COLLATE pg_catalog."default",
    "COUNTY_CODE_DESC" character varying(20) COLLATE pg_catalog."default",
    "COUNTY_PERCENT" numeric(10,2),
    "CITY_CODE" character varying(24) COLLATE pg_catalog."default",
    "CITY_CODE_DESC" character varying(20) COLLATE pg_catalog."default",
    "CITY_PERCENT" numeric(10,2),
    "CITY_SEQ_NUM" integer,
    "FIRE_CODE" character varying(24) COLLATE pg_catalog."default",
    "FIRE_CODE_DESC" character varying(20) COLLATE pg_catalog."default",
    "FIRE_PERCENT" numeric(10,2),
    "FIRE_SEQ_NUM" integer,
    "SPECIAL_CODE" character varying(24) COLLATE pg_catalog."default",
    "SPECIAL_CODE_DESC" character varying(20) COLLATE pg_catalog."default",
    "SPECIAL_PERCENT" numeric(10,2),
    "SPECIAL_SEQ_NUM" integer,
    "TXN_DATE" timestamp(3) without time zone,
    "MODULE_NAME" character varying(100) COLLATE pg_catalog."default",
    "SITUS_TAX_DISTRICT_SEQ_NUM" character varying(100) COLLATE pg_catalog."default"
)
    '''

    view1 = '''
    CREATE OR REPLACE VIEW "LRCCountyWise_PBI"
 AS
 SELECT 'COUNTY WIDE'::text AS "Group",
    count(par."DimParcelPk") AS "ParcelCount",
    sum(COALESCE(par."PARCEL_TOTAL_VALUE", 0::numeric)) AS "TotalValue",
    sum(COALESCE(par."PARCEL_BLDG_VALUE_APPR", 0::numeric)) AS "TotalImprovementValueAppr",
    sum(COALESCE(par."PARCEL_BLDG_VALUE_ASSD", 0::numeric)) AS "TotalImprovementValueAssd",
    sum(COALESCE(par."PARCEL_MISC_IMPRV_VALUE_APPR", 0::numeric)) AS "TotalMiscellaneousImprovementValueAppr",
    sum(COALESCE(par."PARCEL_MISC_IMPRV_VALUE_ASSD", 0::numeric)) AS "TotalMiscellaneousImprovementValueAssd",
    sum(COALESCE(par."PARCEL_LAND_VALUE_APPR", 0::numeric)) AS "Total Land ValueAppr",
    sum(COALESCE(par."PARCEL_LAND_VALUE_ASSD", 0::numeric)) AS "Total Land ValueAssd",
    sum(COALESCE(par."PARCEL_TOTAL_DEF_EXEMPT_VALUE", 0::numeric)) AS "Total Land Deferred Value"
   FROM "LRCParcel" par
  WHERE par."RECORD_STATUS" = 'A'::bpchar AND par."RECORD_ORDER" = 1;

    '''

    view2 = '''
    CREATE OR REPLACE VIEW "LRCParcelWise_PBI"
 AS
 WITH "TJUR" AS (
         SELECT sit."DimSitusPk",
            sit."COUNTY_CODE" AS "CODE",
            sit."COUNTY_CODE_DESC" AS "CODE_DESC",
            sit."COUNTY_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."COUNTY_CODE" IS NOT NULL
        UNION ALL
         SELECT sit."DimSitusPk",
            sit."CITY_CODE" AS "CODE",
            sit."CITY_CODE_DESC" AS "CODE_DESC",
            sit."CITY_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."CITY_CODE" IS NOT NULL
        UNION ALL
         SELECT sit."DimSitusPk",
            sit."FIRE_CODE" AS "CODE",
            sit."FIRE_CODE_DESC" AS "CODE_DESC",
            sit."FIRE_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."FIRE_CODE" IS NOT NULL
        UNION ALL
         SELECT sit."DimSitusPk",
            sit."SPECIAL_CODE" AS "CODE",
            sit."SPECIAL_CODE_DESC" AS "CODE_DESC",
            sit."SPECIAL_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."SPECIAL_CODE" IS NOT NULL
        )
 SELECT tj."CODE" AS "TaxJurisdiction",
    tj."CODE_DESC" AS "TaxJurisdictionDesc",
    par."PARCEL_NUM" AS "Parcel ID",
    par."PIN_NUM" AS "Pin Number",
    par."PIN_EXT" AS "Pin Extension",
    par."MARKET_AREA_ID" AS "Market Area",
    COALESCE(par."PARCEL_TOTAL_VALUE", 0::numeric) AS "TotalValue",
    COALESCE(par."PARCEL_BLDG_VALUE_APPR", 0::numeric) AS "TotalImprovementValueAppr",
    COALESCE(par."PARCEL_BLDG_VALUE_ASSD", 0::numeric) AS "TotalImprovementValueAssd",
    COALESCE(par."PARCEL_MISC_IMPRV_VALUE_APPR", 0::numeric) AS "TotalMiscellaneousImprovementValueAppr",
    COALESCE(par."PARCEL_MISC_IMPRV_VALUE_ASSD", 0::numeric) AS "TotalMiscellaneousImprovementValueAssd",
    COALESCE(par."PARCEL_LAND_VALUE_APPR", 0::numeric) AS "Total Land ValueAppr",
    COALESCE(par."PARCEL_LAND_VALUE_ASSD", 0::numeric) AS "Total Land ValueAssd",
    COALESCE(par."PARCEL_TOTAL_DEF_EXEMPT_VALUE", 0::numeric) AS "Total Land Deferred Value",
    COALESCE(par."PARCEL_TOTAL_LAND_DEF_VALUE", 0::numeric) AS landdeferredvalue,
    0 AS "ImprovementCount",
    par."MISC_IMPRV_COUNT" AS "MiscImprCount",
    par."LANDLINE_COUNT" AS "LandUnitCount"
   FROM "TJUR" tj
     JOIN "LRCParcel" par ON tj."DimSitusPk" = par."DimSitusPk"
  WHERE par."RECORD_STATUS" = 'A'::bpchar AND par."RECORD_ORDER" = 1;
    '''

    view3 = '''
    CREATE OR REPLACE VIEW "LRCTaxJurisdictionWise_PBI"
 AS
 WITH "TJUR" AS (
         SELECT sit."DimSitusPk",
            sit."COUNTY_CODE" AS "CODE",
            sit."COUNTY_CODE_DESC" AS "CODE_DESC",
            sit."COUNTY_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."COUNTY_CODE" IS NOT NULL
        UNION ALL
         SELECT sit."DimSitusPk",
            sit."CITY_CODE" AS "CODE",
            sit."CITY_CODE_DESC" AS "CODE_DESC",
            sit."CITY_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."CITY_CODE" IS NOT NULL
        UNION ALL
         SELECT sit."DimSitusPk",
            sit."FIRE_CODE" AS "CODE",
            sit."FIRE_CODE_DESC" AS "CODE_DESC",
            sit."FIRE_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."FIRE_CODE" IS NOT NULL
        UNION ALL
         SELECT sit."DimSitusPk",
            sit."SPECIAL_CODE" AS "CODE",
            sit."SPECIAL_CODE_DESC" AS "CODE_DESC",
            sit."SPECIAL_PERCENT" AS "PERCENT"
           FROM "LRCSitusTaxDistrict" sit
          WHERE sit."SPECIAL_CODE" IS NOT NULL
        )
 SELECT 'TAX JURISDICTION'::text AS "Group",
    tj."CODE" AS "TaxJurisdiction",
    tj."CODE_DESC" AS "TaxJurisdictionDesc",
    count(par."DimParcelPk") AS "ParcelCount",
    sum(COALESCE(par."PARCEL_TOTAL_VALUE", 0::numeric)) AS "TotalValue",
    sum(COALESCE(par."PARCEL_BLDG_VALUE_APPR", 0::numeric)) AS "TotalImprovementValueAppr",
    sum(COALESCE(par."PARCEL_BLDG_VALUE_ASSD", 0::numeric)) AS "TotalImprovementValueAssd",
    sum(COALESCE(par."PARCEL_MISC_IMPRV_VALUE_APPR", 0::numeric)) AS "TotalMiscellaneousImprovementValueAppr",
    sum(COALESCE(par."PARCEL_MISC_IMPRV_VALUE_ASSD", 0::numeric)) AS "TotalMiscellaneousImprovementValueAssd",
    sum(COALESCE(par."PARCEL_LAND_VALUE_APPR", 0::numeric)) AS "Total Land ValueAppr",
    sum(COALESCE(par."PARCEL_LAND_VALUE_ASSD", 0::numeric)) AS "Total Land ValueAssd",
    sum(COALESCE(par."PARCEL_TOTAL_DEF_EXEMPT_VALUE", 0::numeric)) AS "Total Land Deferred Value"
   FROM "TJUR" tj
     JOIN "LRCParcel" par ON tj."DimSitusPk" = par."DimSitusPk"
  WHERE par."RECORD_STATUS" = 'A'::bpchar AND par."RECORD_ORDER" = 1
  GROUP BY tj."CODE", tj."CODE_DESC";
    '''

    view4 = '''
    CREATE OR REPLACE VIEW cosmosdbdata
 AS
 SELECT DISTINCT p."DimParcelPk",
    p."PARCEL_NUM",
    p."PLAT_BOOK",
    p."PLAT_PAGE",
    ph."BUILDING_TYPE",
    p."TOTAL_VALUE_SQFT",
    o."DimOwnershipPk",
    o."OWNERSHIP_DEED_BOOK",
    o."OWNERSHIP_DEED_PAGE",
    o."MAILING_ADDRESS1",
    ph."SUBDIVISION_NAME",
    ma."TOTAL_LAND_VALUE",
    rb."RES_BLDG_BASEMENT_FINISH_SF",
    rb."RES_BLDG_UNFINISHED_INT_SF",
    rb."RES_BLDG_ASSD_VALUE",
    p."PIN_NUM",
    rbf."RES_BLDG_FIREPLACE",
    rbf."RES_BLDG_USE_DESC",
    p."TOTAL_HEATED_AREA",
    rb."RES_BLDG_BATH_FULL",
    rb."RES_BLDG_BATH_HALF",
    rb."RES_BLDG_BEDROOMS",
    rb."RES_BLDG_YEAR_BUILT",
    o."IS_JAN1_OWNERSHIP",
    p."TOTAL_DEF_VALUE",
    p."MISC_IMPRV_COUNT",
    p."PARCEL_LAND_VALUE_ASSD",
    st."TAX_DISTRICT_DESC",
    ( SELECT jsonb_agg(json_build_object('SALE_DATE', s."SALE_DATE", 'SALE_DEED_BOOK', s."SALE_DEED_BOOK", 'SALE_DEED_PAGE', s."SALE_DEED_PAGE", 'SALE_DATE', s."SALE_DATE", 'SALE_PRICE', s."SALE_PRICE")) AS jsonb_agg
           FROM "LRCSale" s
             JOIN "LRCParcel" par ON s."DimParcelPk" = par."DimParcelPk"
          WHERE par."RECORD_TYPE"::text = 'RCTPSALE'::text AND par."DimParcelPk" = p."DimParcelPk") AS salesdetails,
    ( SELECT jsonb_agg(json_build_object('PERMIT_NUMBER', per."PERMIT_NUMBER", 'PERMIT_ISSUE_DATE', per."PERMIT_ISSUE_DATE", 'PERMIT_ADDRESS', per."PERMIT_ADDRESS", 'PERMIT_NOTES', per."PERMIT_NOTES")) AS jsonb_agg
           FROM "LRCPermit" per
             JOIN "LRCParcel" par ON per."DimParcelPk" = par."DimParcelPk"
          WHERE par."DimParcelPk" = p."DimParcelPk") AS permitdetails
   FROM "LRCParcel" p
     JOIN "LRCOwnership" o ON o."DimOwnershipPk" = p."DimOwnershipPk"
     JOIN "LRCPhysicalAddress" ph ON ph."DimParcelPk" = p."DimParcelPk"
     JOIN "LRCResidentialBuilding" rb ON rb."DimParcelPk" = p."DimParcelPk"
     JOIN "LRCSitusTaxDistrict" st ON st."DimSitusPk" = p."DimSitusPk"
     LEFT JOIN "LRCMarketAreaStatistics" ma ON ma."DimMarketAreaPk" = p."DimMarketAreaPk"
     LEFT JOIN "LRCResidentialBuildingFeature" rbf ON rbf."DimParcelPk" = p."DimParcelPk"
  WHERE p."RECORD_STATUS" = 'A'::bpchar AND p."RECORD_ORDER" = 1 AND rb."RES_BLDG_CARD_NUMBER" = 1::numeric AND rbf."RES_BLDG_FEATURE_SEQ_NUM" = 1 AND rbf."RES_BLDG_FIREPLACE_SEQ_NUM" = 1
  GROUP BY p."DimParcelPk", o."DimOwnershipPk", o."MAILING_ADDRESS1", ph."SUBDIVISION_NAME", ma."TOTAL_LAND_VALUE", rb."RES_BLDG_BASEMENT_FINISH_SF", rb."RES_BLDG_UNFINISHED_INT_SF", rb."RES_BLDG_ASSD_VALUE", p."PIN_NUM", rbf."RES_BLDG_FIREPLACE", rbf."RES_BLDG_USE_DESC", p."TOTAL_HEATED_AREA", rb."RES_BLDG_BATH_FULL", rb."RES_BLDG_BATH_HALF", rb."RES_BLDG_BEDROOMS", rb."RES_BLDG_YEAR_BUILT", o."IS_JAN1_OWNERSHIP", p."TOTAL_DEF_VALUE", st."TAX_DISTRICT_DESC", p."PARCEL_NUM", o."OWNERSHIP_DEED_BOOK", o."OWNERSHIP_DEED_PAGE", p."TOTAL_VALUE_SQFT", p."PLAT_BOOK", p."PLAT_PAGE", p."MISC_IMPRV_COUNT", p."PARCEL_LAND_VALUE_ASSD", ph."BUILDING_TYPE"
 HAVING count(p."DimParcelPk") = 1;
    '''
    v = 0
    ListSql = [table1, table2, table3, table4, table5, table6, table7, table8, table9, view1, view2, view3, view4]
    for i in ListSql:
        v += 1
        try:
            cursor.execute(i)
            # print("Table name : ", v, "  successfully created : ")
        except:
            print("Error occurred in table ", v)
    conn.commit()
    conn.close()


def create_database(database_name):
    HOST = 'https://arist.documents.azure.com:443/'
    MASTER_KEY = 'g40T3IriQjf7YbUGmzms3XGAHpniZz5poWiSa4MNGCtDt9h6gdEhtV9cpwt9sv2vyyM57ZIqpG8QFAVOp7IduQ=='
    client = cosmos_client.CosmosClient(url=HOST, credential=MASTER_KEY, consistency_level="Session")
    # print("\n 1. Create Database")
    try:
        database = client.create_database(database_name)
    except exceptions.CosmosResourceExistsError:
        database = client.get_database_client(database_name)
    create_container(database)


def create_container(db):
    Containers = ['cosmosdbdata', 'DimParcelPk', 'user', 'id']
    i = 0
    while (i < len(Containers)):
        partition_key = PartitionKey(path='/' + Containers[i + 1], kind='Hash')
        # print("\n Create Container with name = ",Containers[i])
        try:
            db.create_container(id=Containers[i], partition_key=partition_key)
            # print(
            #     'Container with id \'{0}\' and primary key is \'{1}\' created'.format(Containers[i], Containers[i + 1]))
        except exceptions.CosmosResourceExistsError:
            print('A container with id \'{0}\' already exists'.format(Containers[i]))
        finally:
            i += 2


def get_access_token(application_id, application_secret, user_id, user_password):
    data = {
        'grant_type': 'password',
        'scope': 'https://analysis.windows.net/powerbi/api/.default',
        'resource': "https://analysis.windows.net/powerbi/api",
        'client_id': application_id,
        'client_secret': application_secret,
        'username': user_id,
        'password': user_password
    }
    token = requests.post("https://login.microsoftonline.com/common/oauth2/token", data=data)
    assert token.status_code == 200, "Fail to retrieve token: {}".format(token.text)
    # print("Got access token: ")
    return token.json()['access_token']


def make_headers(application_id, application_secret, user_id, user_password):
    return {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': "Bearer {}".format(
            get_access_token(application_id, application_secret, user_id, user_password))
    }


def get_embed_token_report(application_id, application_secret, user_id, user_password, group_id, report_id):
    endpoint = "https://api.powerbi.com/v1.0/myorg/groups/{}/reports/{}/GenerateToken".format(group_id, report_id)
    headers = make_headers(application_id, application_secret, user_id, user_password)
    res = requests.post(endpoint, headers=headers, json={"accessLevel": "View"})
    return res.json()['token']


def get_groups(APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    endpoint = "https://api.powerbi.com/v1.0/myorg/groups"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.get(endpoint, headers=headers).json()


def get_gateways(datasetId, APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    endpoint = "https://api.powerbi.com/v1.0/myorg/datasets/" + datasetId + "/Default.GetBoundGatewayDatasources"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.get(endpoint, headers=headers).json()


def create_groups(name, APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    endpoint = "https://api.powerbi.com/v1.0/myorg/groups"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.post(endpoint, headers=headers, json={'name': name})


def create_dataset(id, body, APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + id + "/datasets"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.post(endpoint, headers=headers, json=body)


def create_rowsInDataset(groupId, datasetId, tableName, body, APPLICATION_ID, APPLICATION_SECRET, USER_ID,
                         USER_PASSWORD):
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + groupId + "/datasets/" + datasetId + "/tables/" + tableName + "/rows"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.post(endpoint, headers=headers, json=body)


def insert_data_into_rows(groupId, datasetId, postgresTableName, datasourceTableName, APPLICATION_ID,
                          APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    stmt = 'select row_to_json(row) from (SELECT * FROM ' + postgresTableName + ') row;'
    # stmt = 'select row_to_json(row) from (SELECT * FROM rutherford."LRCCountyWise_PBI") row;'
    rowsB = run(stmt)
    v = []
    last = len(rowsB)
    if len(rowsB) > 10000:
        last = 10000
        bigTableDataLoad(0, last, v, groupId, datasetId, datasourceTableName, len(rowsB), rowsB, APPLICATION_ID,
                         APPLICATION_SECRET, USER_ID, USER_PASSWORD)
        return
    else:
        last = len(rowsB)

    for i in range(0, last):
        v.append(rowsB[i][0])
    winner = {
        "rows": v
    }
    rowsBody = winner
    rowfinal = create_rowsInDataset(groupId, datasetId, datasourceTableName, rowsBody, APPLICATION_ID,
                                    APPLICATION_SECRET, USER_ID, USER_PASSWORD)


def bigTableDataLoad(start, last, v, groupId, datasetId, datasourceTableName, length, rowsB, APPLICATION_ID,
                     APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    v = []
    if (start >= length):
        return
    else:
        for i in range(start, last):
            v.append(rowsB[i][0])
        winnner = {
            "rows": v
        }
        rowsBody = winnner
        rowfinal = create_rowsInDataset(groupId, datasetId, datasourceTableName, rowsBody, APPLICATION_ID,
                                        APPLICATION_SECRET, USER_ID, USER_PASSWORD)
        start = last
        last += 9500
        if (last > length):
            last = length
        bigTableDataLoad(start, last, v, groupId, datasetId, datasourceTableName, length, rowsB, APPLICATION_ID,
                         APPLICATION_SECRET, USER_ID, USER_PASSWORD)


def run(stmt):
    cur = psycopg2.connect(database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
                           host='aristanalyticspoc.postgres.database.azure.com', port='5432').cursor()
    cur.execute(stmt)
    result = cur.fetchall()
    return result


def cloneReport(groupId, groupIdFromClone, APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    body = {
        "name": "report1",
        "targetWorkspaceId": groupId
    }
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    reportId = "61bc1ea2-7465-4518-884c-53438bdf2b7d"  # 1160
    endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + groupIdFromClone + "/reports/" + reportId + "/Clone"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.post(endpoint, headers=headers, json=body)


def cloneReportRebind(groupId, newCreatedReportId, datasetId, APPLICATION_ID, APPLICATION_SECRET, USER_ID,
                      USER_PASSWORD):
    body = {
        "datasetId": datasetId
    }
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    # # endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + groupId + "/datasets/" + datasetId + "/tables/" + tableName + "/rows"
    # endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + groupId + "/reports/ffb86feb-5e1a-4f59-9f97-21b53249bd63/Clone"
    # endpoint = "https://api.powerbi.com/v1.0/myorg/groups/"+groupIdFromClone+"/reports/ffb86feb-5e1a-4f59-9f97-21b53249bd63/Clone"
    endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + groupId + "/reports/" + newCreatedReportId + "/Rebind"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.post(endpoint, headers=headers, json=body)


def get_reports_scripts(APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD):
    application_id = APPLICATION_ID
    application_secret = APPLICATION_SECRET
    user_id = USER_ID
    user_password = USER_PASSWORD
    endpoint = "https://api.powerbi.com/v1.0/myorg/reports"
    headers = make_headers(application_id, application_secret, user_id, user_password)
    return requests.get(endpoint, headers=headers)


def PowerBiAutoMationAPI(name):
    records = []
    RESOURCE = "https://analysis.windows.net/powerbi/api"  # Don't change that.
    APPLICATION_ID = "3c2004e3-1b41-4051-aecc-b7f9a4cfb78b"  # The ID of the application in Active Directory
    APPLICATION_SECRET = "1OV8Q~NcExwdXaS90rnPkCdmAZhxjghrLhUencRv"  # A valid key for that application in Active Directory

    USER_ID = "shubhampatil@techmenttech123.onmicrosoft.com"  # A user that has access to PowerBI and the application
    USER_PASSWORD = "c=JcoV-LU%f7mkt-"  # The password for that user
    tableNameInStart = ""
    postgresDatasetSchemaName = name
    groupIdForClone = "3ab6cb6d-323e-4108-a749-e8a8799358fd"

    api_response = create_groups(name, APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
    records.append(api_response.json()["id"])  # Adding group Id
    json1 = {
        "name": "farrgut",
        "defaultMode": "Push",
        "tables": [
            {
                "name": tableNameInStart + "LRCCountyWise_PBI",
                "columns": [
                    {
                        "name": "Group",
                        "dataType": "string"
                    },
                    {
                        "name": "ParcelCount",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land Deferred Value",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land ValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land ValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalImprovementValue",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalImprovementValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalImprovementValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalMiscellaneousImprovementValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalMiscellaneousImprovementValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalValue",
                        "dataType": "Int64"
                    }
                ]
            },
            {
                "name": tableNameInStart + "LRCTaxJurisdictionWise_PBI",
                "columns": [
                    {
                        "name": "Group",
                        "dataType": "string"
                    },
                    {
                        "name": "TaxJurisdiction",
                        "dataType": "string"
                    },
                    {
                        "name": "TaxJurisdictionDesc",
                        "dataType": "string"
                    },
                    {
                        "name": "ParcelCount",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land Deferred Value",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land ValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land ValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalImprovementValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalImprovementValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalMiscellaneousImprovementValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalMiscellaneousImprovementValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalValue",
                        "dataType": "Int64"
                    }
                ]
            },
            {
                "name": tableNameInStart + "LRCParcelWise_PBI",
                "columns": [
                    {
                        "name": "TaxJurisdiction",
                        "dataType": "string"
                    },
                    {
                        "name": "TaxJurisdictionDesc",
                        "dataType": "string"
                    },
                    {
                        "name": "Parcel ID",
                        "dataType": "string"
                    },
                    {
                        "name": "Pin Number",
                        "dataType": "string"
                    },
                    {
                        "name": "Pin Extension",
                        "dataType": "string"
                    },
                    {
                        "name": "Market Area",
                        "dataType": "string"
                    },
                    {
                        "name": "Total Land Deferred Value",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land ValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Total Land ValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalImprovementValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalImprovementValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalMiscellaneousImprovementValueAppr",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalMiscellaneousImprovementValueAssd",
                        "dataType": "Int64"
                    },
                    {
                        "name": "TotalValue",
                        "dataType": "Int64"
                    },
                    {
                        "name": "landdeferredvalue",
                        "dataType": "Int64"
                    },
                    {
                        "name": "ImprovementCount",
                        "dataType": "Int64"
                    },
                    {
                        "name": "MiscImprCount",
                        "dataType": "Int64"
                    },
                    {
                        "name": "LandUnitCount",
                        "dataType": "Int64"
                    }
                ]
            }
        ]
    }
    dataset = create_dataset(api_response.json()["id"], json1, APPLICATION_ID, APPLICATION_SECRET, USER_ID,
                             USER_PASSWORD)
    records.append(dataset.json()["id"])  # Adding datasetId
    tableName = tableNameInStart + "LRCCountyWise_PBI"
    postgresTableName = postgresDatasetSchemaName + '."LRCCountyWise_PBI"'
    insert_data_into_rows(api_response.json()["id"], dataset.json()["id"], postgresTableName, tableName, APPLICATION_ID,
                          APPLICATION_SECRET, USER_ID, USER_PASSWORD)

    # second table data insert
    tableName = tableNameInStart + "LRCParcelWise_PBI"
    postgresTableName = postgresDatasetSchemaName + '."LRCParcelWise_PBI"'
    insert_data_into_rows(api_response.json()["id"], dataset.json()["id"], postgresTableName, tableName, APPLICATION_ID,
                          APPLICATION_SECRET, USER_ID, USER_PASSWORD)

    # third table data insert
    postgresTableName = postgresDatasetSchemaName + '."LRCTaxJurisdictionWise_PBI"'
    tableName = tableNameInStart + "LRCTaxJurisdictionWise_PBI"
    insert_data_into_rows(api_response.json()["id"], dataset.json()["id"], postgresTableName, tableName, APPLICATION_ID,
                          APPLICATION_SECRET, USER_ID, USER_PASSWORD)

    reportClone = cloneReport(api_response.json()["id"], groupIdForClone, APPLICATION_ID, APPLICATION_SECRET, USER_ID,
                              USER_PASSWORD)
    records.append(reportClone.json()["id"])  # Adding report Id
    Abhishek = cloneReportRebind(api_response.json()["id"], reportClone.json()["id"], dataset.json()["id"],
                                 APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
    print(Abhishek)
    # webbrowser.open("https://app.powerbi.com/reportEmbed?reportId=" + reportClone.json()[
    #     "id"] + "&autoAuth=true&ctid=d99798d5-98a3-4900-9e12-bcbce1aa8e27")
    records.append("https://app.powerbi.com/reportEmbed?reportId=" + reportClone.json()[
        "id"] + "&autoAuth=true&ctid=d99798d5-98a3-4900-9e12-bcbce1aa8e27")  # Adding I Frame URL
    records.append(reportClone.json()['embedUrl'])
    return records


def InsertIntoDataBaseCountyDetails(records, username, encPassword, countyName, uniqueId):
    conn = psycopg2.connect(
        database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com', port='5432'
    )
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    dt = datetime.datetime.utcnow()
    schemaSet = "set schema 'tenant_details'"
    cursor.execute(schemaSet)
    query1 = ''' INSERT INTO tenant_details.test_tbl_user( county_unique_id, username, county_name, password, created_at, active)
    	VALUES (%s, %s, %s, %s,%s,%s);'''
    record_to_insert1 = (uniqueId, username, countyName, encPassword, dt, False)
    cursor.execute(query1, record_to_insert1)
    query2 = ''' INSERT INTO tenant_details.test_tbl_county_details(
	unique_id, iframe, group_id, report_id, dataset_id, mliframe_web, embed_url_web_bi, group_id_ml, report_id_ml_mobile, dataset_id_ml, embed_url_mobile_ml, embed_url_web_ml, report_id_ml_web, mliframe_mobile)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); '''
    record_to_insert2 = (uniqueId,None,None,None,None,None,None,None,None,None,None,None,None,None)
    cursor.execute(query2, record_to_insert2)
    conn.commit()
    conn.close()


def CreateTrigger(name):
    subscription_id = '7e9b414e-ced3-4b74-80bf-ea178f9df741'
    rg_name = 'aristpoc'
    df_name = 'aristpoc'
    credentials = ClientSecretCredential(client_id='2b252a12-a1e5-475c-9cd7-2592c01442c8',
                                         client_secret='vLB8Q~sVfJguC2kgth_IDFdqC.iz-1oH1Ue4Ha9U',
                                         tenant_id='fcaf9f5c-381a-4b3f-92d3-8a1f99edef15')

    resource_client = ResourceManagementClient(credentials, subscription_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    # tr_name = 'trigger5Abhishek'
    tr_name = 'triggerfor' + name
    print("trigger name is : ", tr_name)
    # dateTime = datetime.now()
    # print(dateTime)
    utc_time = datetime.datetime.utcnow() + timedelta(minutes=2)
    utc_time1 = utc_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(utc_time1)
    scheduler_recurrence = ScheduleTriggerRecurrence(frequency='Day', interval='1', start_time=utc_time1,
                                                     time_zone='UTC')
    pipeline_parameters = {'sqlserver': 'aristpoc.database.windows.net',
                           'sqldatabase': 'rutherford',
                           'sqlusername': 'techment',
                           'sqlpswd': 'Sanjeev@123',
                           'psqlschema': name,
                           'psqlstagingschema': name + '_staging'}
    pipelines_to_run = []
    pipeline_reference = PipelineReference(reference_name='Dynamic_MultiViewsUpsert', )
    pipelines_to_run.append(
        TriggerPipelineReference(pipeline_reference=pipeline_reference, parameters=pipeline_parameters))
    tr_properties = TriggerResource(
        properties=ScheduleTrigger(description='My scheduler trigger', pipelines=pipelines_to_run,
                                   recurrence=scheduler_recurrence))
    adf_client.triggers.create_or_update(rg_name, df_name, tr_name, tr_properties)
    adf_client.triggers.begin_start(rg_name, df_name, tr_name)
    print("Trigger created completed")


def signUp(username, countyName, password):
    encPassword = PasswordEncrypt(password)
    uniqueId = GenerateUniqueId()
    createTablesAndSchemaPostgres(countyName.lower())
    createTablesAndSchemaPostgres(countyName.lower() + "_staging")
    create_database(countyName.lower())
    CreateTrigger(countyName.lower())
    records = []
    InsertIntoDataBaseCountyDetails(records, username, encPassword, countyName, uniqueId)
    print("Created The Schemas and Process is done")


def PasswordEncrypt(password):
    key = b't10dtXImHwWp6pcc7GI0ydkX5l-JPyKV-CLvk8QPQh4='
    # print(key)
    fernet = Fernet(key)
    encPassword = fernet.encrypt(password.encode())
    return encPassword.decode()


def DecodePassword(password):
    key = b't10dtXImHwWp6pcc7GI0ydkX5l-JPyKV-CLvk8QPQh4='
    fernet = Fernet(key)
    encPassword = fernet.decrypt(password).decode()
    return encPassword


def GenerateUniqueId():
    S = 15  # number of characters in the string.
    # call random.choices() string module to find the string in Uppercase + numeric data.
    ran = ''.join(random.choices(string.ascii_uppercase + string.digits, k=S))
    print("The randomly generated string is : " + str(ran))  # print the random data
    return str(ran)


class RegisterAPI(APIView):

    def post(self, request):
        username = request.data["username"]
        county = request.data["county"]
        password = request.data["password"]
        try:
            signUp(username, county.lower(), password)
            return Response({"message": "success"}, status=status.HTTP_200_OK)
        except Exception as e:
            print(e)
            print("No")
            return Response({"message": "Failed to register"}, status=status.HTTP_400_BAD_REQUEST)


def getAllCounties():
    conn = psycopg2.connect(
        database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com', port='5432'
    )
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    string = "SET SCHEMA 'tenant_details'"
    cursor.execute(string)
    sql = '''select * from test_tbl_user ORDER BY county_name ASC'''
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.commit()
    conn.close()
    a = []
    for i in result:
        a.append(i[2])
    return a


class AllCounty(APIView):
    def get(self, request):
        try:
            v = getAllCounties()
            return Response(v, status=status.HTTP_200_OK)
        except:
            print("No")
            return Response({"message": "Failed to register"}, status=status.HTTP_400_BAD_REQUEST)


def UpdateValueInDBForBI(uniqueId, records):
    conn = psycopg2.connect(
        database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com', port='5432'
    )
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    schemaSet = "set schema 'tenant_details'"
    cursor.execute(schemaSet)
    print("Updating in table started ")
    query1 = ''' UPDATE test_tbl_county_details
    SET iframe = %s,
    group_id = %s,
    report_id = %s,
    dataset_id = %s,
    embed_url_web_bi = %s
    WHERE unique_id = %s; '''
    record_to_insert1 = (records[3], records[0], records[2], records[1], records[4], uniqueId)
    cursor.execute(query1, record_to_insert1)
    # query2 = ''' Update test_tbl_user
    # set active = %s
    # where county_unique_id = %s ;
    # '''
    # print("Updating in table running ")
    # record_to_insert2 = (True, uniqueId)
    # cursor.execute(query2, record_to_insert2)
    print("Updating in table Completed ")
    conn.commit()
    conn.close()


def UpdateValueInDBForML(uniqueId, records):
    conn = psycopg2.connect(
        database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com', port='5432'
    )

    # returnResult = [workspace_id, dataset_id, reportClone.json()["id"], reportCloneMobile.json()["id"], iframe1,
    #                 iframe2, reportClone.json()['embedUrl'], reportCloneMobile.json()['embedUrl']]
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    schemaSet = "set schema 'tenant_details'"
    cursor.execute(schemaSet)
    print("Updating in table started ")
    query1 = ''' UPDATE tenant_details.test_tbl_county_details
	SET group_id_ml=%s,
	 dataset_id_ml=%s,
	 report_id_ml_web=%s,
	 report_id_ml_mobile=%s,
	  mliframe_web=%s,
	  mliframe_mobile = %s,
	   embed_url_web_ml=%s,
	   embed_url_mobile_ml=%s 
	WHERE unique_id=%s;'''
    record_to_insert1 = (
        records[0],
        records[1],
        records[2],
        records[3],
        records[4],
        records[5],
        records[6],
        records[7],
        uniqueId)
    cursor.execute(query1, record_to_insert1)
    query2 = ''' Update test_tbl_user
    set active = %s
    where county_unique_id = %s ;
    '''
    print("Updating in table running ")
    record_to_insert2 = (True, uniqueId)
    cursor.execute(query2, record_to_insert2)
    print("Updating in table Completed ")
    conn.commit()
    conn.close()


def MLModel(countyName):
    import psycopg2
    import pandas as pd
    import numpy as np
    from sklearn.preprocessing import MinMaxScaler
    from keras.preprocessing.sequence import TimeseriesGenerator
    from keras.models import Sequential
    from keras.layers import Dense
    from keras.layers import LSTM
    from keras.layers import Dropout
    from pandas.tseries.offsets import DateOffset
    import webbrowser
    import requests
    import psycopg2
    import json
    import time
    import csv
    import math

    # Establishing the connection

    conn = psycopg2.connect(
        database="AristPOC",
        user='techment@aristanalyticspoc',
        password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com',
        port='5432'
    )

    schema = countyName
    cur = conn.cursor()
    cur.execute('SELECT * FROM ' + schema + '."LRCSale"')
    cdata = cur.fetchall()
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    originalData = pd.DataFrame(data=cdata, columns=cols)
    selectedColumns = originalData[["SALE_PK", "SALE_DATE"]]
    selectedColumns['Months'] = selectedColumns['SALE_DATE'].dt.to_period('M')
    sortedData = selectedColumns.sort_values(by=['SALE_DATE'])
    filteredData = sortedData[(sortedData['SALE_DATE'] >= '2014-01-01') & (sortedData['SALE_DATE'] < '2022-01-01')]
    filteredData = filteredData.drop_duplicates(subset='SALE_PK', keep="first")
    organizedData = filteredData.groupby(['Months']).agg(count=('Months', 'count')).reset_index()
    organizedData['Months'] = organizedData['Months'].astype('datetime64[ns]')
    organizedData = organizedData.resample('MS', on='Months').mean()
    organizedData['NUMBER_OF_SALES'] = organizedData['count'].astype(int)
    organizedData.drop('count', axis=1, inplace=True)

    # Importing Data from source
    train = organizedData

    scaler = MinMaxScaler()
    scaler.fit(train)
    train = scaler.transform(train)

    n_input = 12
    n_features = 1
    generator = TimeseriesGenerator(train, train, length=n_input, batch_size=6)

    # model training
    model = Sequential()
    model.add(LSTM(200, activation='relu', input_shape=(n_input, n_features)))
    model.add(Dropout(0.15))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')
    history = model.fit_generator(generator, epochs=10, verbose=1)

    pred_list = []
    batch = train[-n_input:].reshape((1, n_input, n_features))
    for i in range(n_input):
        pred_list.append(model.predict(batch)[0])
        batch = np.append(batch[:, 1:, :], [[pred_list[i]]], axis=1)

    add_dates = [organizedData.index[-1] + DateOffset(months=x) for x in range(0, 13)]
    future_dates = pd.DataFrame(index=add_dates[1:], columns=organizedData.columns)

    # removing scale

    df_predict = pd.DataFrame(scaler.inverse_transform(pred_list),
                              index=future_dates[-n_input:].index, columns=['Prediction'])
    df_proj = pd.concat([organizedData, df_predict], axis=1)

    prediction = df_proj.reset_index()

    prediction.rename(columns={'index': 'SALE_DATE'}, inplace=True)

    y_lower = []
    y_higher = []
    for i in prediction.index:
        j = prediction['Prediction'][i]
        y_lower.append(j - j * (10 / 100))
        y_higher.append(j + j * (10 / 100))

    prediction['forecast_lower'] = y_lower
    prediction['forecast_upper'] = y_higher
    prediction['Years'] = prediction['SALE_DATE'].dt.to_period('Y')
    predictionWithforecastInterval = prediction

    predictionWithforecastInterval['Prediction'] = predictionWithforecastInterval['Prediction'].fillna(0)
    predictionWithforecastInterval['NUMBER_OF_SALES'] = predictionWithforecastInterval['NUMBER_OF_SALES'].fillna(0)
    predictionWithforecastInterval["NUMBER_OF_SALES"] = predictionWithforecastInterval["NUMBER_OF_SALES"] + \
                                                        predictionWithforecastInterval["Prediction"]
    predictionWithforecastInterval.drop("Prediction", axis='columns', inplace=True)
    outputPrediction = predictionWithforecastInterval

    average = (
        outputPrediction.groupby(outputPrediction.SALE_DATE.dt.year)['NUMBER_OF_SALES'].transform('mean')).tolist()
    av = pd.DataFrame(average)
    av[0] = av[0].pct_change() * 100
    av[0] = av[0].fillna(0).astype(np.int64, errors='ignore')
    outputPrediction['Percent_Change_Per_Year'] = av
    outputWithPrecentageYear = outputPrediction

    outputWithPrecentageYear['Years'] = outputWithPrecentageYear['Years'].astype(str)
    outputWithPrecentageYear['Quarters'] = outputWithPrecentageYear['SALE_DATE'].dt.quarter
    outputWithPrecentageYear['SALE_DATE'] = outputWithPrecentageYear['SALE_DATE'].astype(str)
    outputWithPrecentageYear['Quarters'] = outputWithPrecentageYear['Quarters'].astype(str)
    outWithQuarters = outputWithPrecentageYear

    outWithQuarters['Percent_Change_Per_Year'] = outWithQuarters['Percent_Change_Per_Year'].astype(str)
    outWithQuarters['Percent_Change_Per_Year'] = outWithQuarters['Percent_Change_Per_Year'].replace('0', None)
    outWithQuarters['NUMBER_OF_SALES'] = outWithQuarters['NUMBER_OF_SALES'].astype(int)
    outWithQuarters['NUMBER_OF_SALES'] = outWithQuarters['NUMBER_OF_SALES'].astype(str)
    outputforReport = outWithQuarters

    outputforReport['forecast_lower'] = outputforReport['forecast_lower'].fillna(0).astype(np.int64, errors='ignore')
    outputforReport['forecast_upper'] = outputforReport['forecast_upper'].fillna(0).astype(np.int64, errors='ignore')
    # outputforReport['forecast_upper'] = outputforReport['forecast_upper'].replace(0, None)
    # outputforReport['forecast_lower'] = outputforReport['forecast_lower'].replace(0, None)
    outputforReport.rename(columns={'NUMBER_OF_SALES': 'SALE_PRICE'}, inplace=True)
    dataReport = outputforReport.to_dict(orient='records')

    RESOURCE = "https://analysis.windows.net/powerbi/api"  # Don't change that.
    APPLICATION_ID = "15e1c891-fbd5-4f44-8ace-11064073cb48"  # The ID of the application in Active Directory
    APPLICATION_SECRET = "LoZ8Q~KCRqSe-9oGXYlIJzj4zeon1ZO70V3onaTv"  # A valid key for that application in Active Directory

    USER_ID = "shubhampatil@techmenttech123.onmicrosoft.com"  # A user that has access to PowerBI and the application
    USER_PASSWORD = "c=JcoV-LU%f7mkt-"  # The password for that user

    def get_access_token(application_id, application_secret, user_id, user_password):
        data = {
            'grant_type': 'password',
            'scope': 'https://analysis.windows.net/powerbi/api/.default',
            'resource': "https://analysis.windows.net/powerbi/api",
            'client_id': application_id,
            'client_secret': application_secret,
            'username': user_id,
            'password': user_password
        }
        token = requests.post("https://login.microsoftonline.com/common/oauth2/token", data=data)
        assert token.status_code == 200, "Fail to retrieve token: {}".format(token.text)
        # print("Got access token: ")
        return token.json()['access_token']

    def make_headers(application_id, application_secret, user_id, user_password):
        return {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': "Bearer {}".format(
                get_access_token(application_id, application_secret, user_id, user_password))
        }

    def get_embed_token_report(application_id, application_secret, user_id, user_password, group_id, report_id):
        endpoint = "https://api.powerbi.com/v1.0/myorg/groups/{}/reports/{}/GenerateToken".format(group_id, report_id)
        headers = make_headers(application_id, application_secret, user_id, user_password)
        res = requests.post(endpoint, headers=headers, json={"accessLevel": "View"})
        return res.json()['token']

    def get_groups():
        endpoint = "https://api.powerbi.com/v1.0/myorg/groups"
        headers = make_headers(APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
        return requests.get(endpoint, headers=headers).json()

    def cloneReport(target_group_id, source_group_id, source_report_id):
        body = {
            "name": 'lstm',
            "targetWorkspaceId": target_group_id
        }
        application_id = APPLICATION_ID
        application_secret = APPLICATION_SECRET
        user_id = USER_ID
        user_password = USER_PASSWORD
        endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + source_group_id + "/reports/" + source_report_id + "/Clone"

        headers = make_headers(application_id, application_secret, user_id, user_password)
        return requests.post(endpoint, headers=headers, json=body)

    def cloneMobileViewReport(target_group_id, source_group_id, source_mobile_report_id):
        body = {
            "name": "lstm_mobile_view",
            "targetWorkspaceId": target_group_id
        }
        endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + source_group_id + "/reports/" + source_mobile_report_id + "/Clone"
        headers = make_headers(APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
        return requests.post(endpoint, headers=headers, json=body)

    def cloneReportRebind(groupId, clonedReportID, datasetId):
        body = {
            "datasetId": datasetId
        }
        application_id = APPLICATION_ID
        application_secret = APPLICATION_SECRET
        user_id = USER_ID
        user_password = USER_PASSWORD
        endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + groupId + "/reports/" + clonedReportID + "/Rebind"
        headers = make_headers(application_id, application_secret, user_id, user_password)
        return requests.post(endpoint, headers=headers, json=body)

    def cloneMobileReportRebind(group_id, clone_mobile_report_id, dataset_id):
        body = {
            "datasetId": dataset_id
        }
        endpoint = "https://api.powerbi.com/v1.0/myorg/groups/" + group_id + "/reports/" + clone_mobile_report_id + "/Rebind"
        headers = make_headers(APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
        return requests.post(endpoint, headers=headers, json=body)

    name = schema + '_Workspace'

    group_name = create_groups(name, APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
    workspace_id = group_name.json()["id"]

    json = {
        "name": "Prediction Model",
        "defaultMode": "Push",
        "tables": [
            {
                "name": "rutherford",
                "columns": [
                    {
                        "name": "SALE_DATE",
                        "dataType": "DateTime"
                    },
                    {
                        "name": "SALE_PRICE",
                        "dataType": "Decimal"
                    },
                    {
                        "name": "forecast_lower",
                        "dataType": "Decimal"
                    },
                    {
                        "name": "forecast_upper",
                        "dataType": "Decimal"
                    },
                    {
                        "name": "Years",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Percent_Change_Per_Year",
                        "dataType": "Int64"
                    },
                    {
                        "name": "Quarters",
                        "dataType": "Int64"
                    }
                ]
            }
        ]
    }

    dataset = create_dataset(workspace_id, json, APPLICATION_ID, APPLICATION_SECRET, USER_ID, USER_PASSWORD)
    dataset.json()
    dataset_id = dataset.json()["id"]

    obj = {"rows": dataReport}
    # print(obj)
    create_rowsInDataset(workspace_id, dataset_id, "rutherford", obj, APPLICATION_ID, APPLICATION_SECRET, USER_ID,
                         USER_PASSWORD)
    target_group_id = workspace_id
    source_group_id = "6c6a2de6-403c-4bd1-8e76-d6e22c0b5b35"
    source_report_id = "55df3f83-5ff9-4981-bf6a-fa95094d4970"
    source_mobile_report_id = "8911cc52-67c8-4feb-a431-c9b0ae0067e7"
    reportClone = cloneReport(target_group_id, source_group_id, source_report_id)
    # report Id web appending
    reportCloneMobile = cloneMobileViewReport(target_group_id, source_group_id, source_mobile_report_id)
    # report Id mobile Append
    print("=======================Web Report==================", "\n", reportClone.json())
    print("=====================Mobile Report==================", "\n", reportCloneMobile.json())
    cloneReportRebind(workspace_id, reportClone.json()["id"], dataset.json()["id"])
    cloneMobileReportRebind(workspace_id, reportCloneMobile.json()["id"], dataset.json()["id"])

    iframe1 = "https://app.powerbi.com/reportEmbed?reportId=" + reportClone.json()[
        "id"] + "&autoAuth=true&ctid=d99798d5-98a3-4900-9e12-bcbce1aa8e27"

    iframe2 = "https://app.powerbi.com/reportEmbed?reportId=" + reportCloneMobile.json()[
        "id"] + "&autoAuth=true&ctid=d99798d5-98a3-4900-9e12-bcbce1aa8e27"

    returnResult = [workspace_id, dataset_id, reportClone.json()["id"], reportCloneMobile.json()["id"], iframe1,
                    iframe2, reportClone.json()['embedUrl'], reportCloneMobile.json()['embedUrl']]
    return returnResult


def AutomationOfBI():
    conn = psycopg2.connect(
        database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com', port='5432'
    )
    cursor = conn.cursor()
    query1 = 'select * from tenant_details.test_tbl_user where active=false '
    cursor.execute(query1)
    record = cursor.fetchall()
    for i in record:
        if (datetime.datetime.utcnow()) - timedelta(minutes=30) > i[4]:
            details = PowerBiAutoMationAPI(i[2])
            print("Automated BI sucessfully Completed : ")
            UpdateValueInDBForBI(i[0], details)
    conn.commit()
    conn.close()


def AutomationOfML():
    conn = psycopg2.connect(
        database="AristPOC", user='techment@aristanalyticspoc', password='Sanjeev@123',
        host='aristanalyticspoc.postgres.database.azure.com', port='5432'
    )
    cursor = conn.cursor()
    query1 = 'select * from tenant_details.test_tbl_user where active=false '
    cursor.execute(query1)
    record = cursor.fetchall()
    conn.commit()
    conn.close()
    for i in record:
        if (datetime.datetime.utcnow()) - timedelta(minutes=30) > i[4]:
            try:
                details = MLModel(i[2])
                UpdateValueInDBForML(i[0], details)
            except Exception as e:
                print("Exception is : ", str(e))
            print("Automated ML sucessfully Completed : ")


class CreationOfBI(APIView):

    def post(self, request):
        try:
            AutomationOfBI()
            AutomationOfML()
            return Response({"message": "success"}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"message": e}, status=status.HTTP_400_BAD_REQUEST)


class CreationOfML(APIView):
    def post(self, request):
        try:
            AutomationOfML()
            return Response({"message": "success"}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"message": e}, status=status.HTTP_400_BAD_REQUEST)
