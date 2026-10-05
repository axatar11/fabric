#!/usr/bin/env python
# coding: utf-8

# ## medallion_config
# 
# New notebook

# In[ ]:


"""Catalog, table, and business constants for Bronze / Silver / Gold notebooks."""

# --- Lakehouse / warehouse catalogs ---

WORKSPACEID  = "4cecee03-0629-4081-8ae8-b749ee04d43f"
LAKEHOUSE_WS = "4cecee03-0629-4081-8ae8-b749ee04d43f"

BRONZE_DATABASE = "AI_Medallion_Bronze"
SILVER_DATABASE = "AI_Medallion"
GOLD_DATABASE = "AI_Medallion"

SILVER_SCHEMA = "Silver"
GOLD_SCHEMA = "Gold"

BRONZE_MEDALLION_DATABASE = "AI_Medallion"
BRONZE_CURSOR_SCHEMA = "Bronze"

# Reference / lookup
COUNTRY_DATABASE = "AI_Medallion_Silver"
COUNTRY_TABLE = "CoreCountry_Excel_Hours_Historic"

# --- Bronze sources ---
TABLE_HC_BRONZE = f"{BRONZE_DATABASE}.dbo.HC_Bronze_Historical"
TABLE_OKTA_BRONZE = f"{BRONZE_DATABASE}.dbo.CoreOktaUser"
TABLE_CURSOR_BRONZE = f"{BRONZE_MEDALLION_DATABASE}.{BRONZE_CURSOR_SCHEMA}.cursor_usage"
TABLE_COUNTRY = f"{COUNTRY_DATABASE}.dbo.{COUNTRY_TABLE}"

# --- Silver targets / sources ---
TABLE_HC_SILVER = f"{SILVER_DATABASE}.{SILVER_SCHEMA}.HC_Silver_Historical"
TABLE_OKTA_SILVER = f"{SILVER_DATABASE}.{SILVER_SCHEMA}.Okta_User_for_AI"
TABLE_CURSOR_SILVER = f"{SILVER_DATABASE}.{SILVER_SCHEMA}.cursor_usage"

# --- Gold targets ---
TABLE_FACT_CURSOR_ACTIVE = f"{GOLD_DATABASE}.{GOLD_SCHEMA}.Fact_Cursor_Active"

# --- Shared business rules ---
NO_COUNTRY = "No Country"
HC_DIVISION_INC_DEFAULT = "Others"

# --- FactCursorActive (Gold) constants ---
CURSOR_ASSET_ID = "PENDING"
CURSOR_ASSET_NAME = "Cursor"
CURSOR_USE_CASE_CODE = "UC-9006-CURS-001"
CURSOR_USE_CASE_DISPLAY_NAME = "Active"
CURSOR_USE_CASE_MULTIPLIER = 1
CURSOR_AUTOMATION_POC = 1
CURSOR_TOTAL_HOURS_SAVED_POC = 0

# Incremental merge: one fact row per silver usage event (stable hash of silver grain)
FACT_CURSOR_ACTIVE_MERGE_KEY = "RecordSurrogateKey"

