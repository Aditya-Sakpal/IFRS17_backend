from models.calculation import Run, CoverageUnitsRec,CashFlow,Liability_Init_Rec,Rec_Bel_Updated,RunInput


# DB_URL="postgresql://infigos_solutions_user:U640zVayfFvOTg4S6U7hN14dy2UoVHBS@dpg-ctiudvdumphs73f70t90-a.singapore-postgres.render.com/infigos_solutions"

# DB_URL = "postgresql://user:J6TmlGJ0DWzyUGEucZh4fJ40AT0Iwq7V@dpg-cvcp4mt2ng1s73913fo0-a.singapore-postgres.render.com/infigos_db"
# DB_URL = "postgresql://postgres:postgres@localhost:5432/infigos_db_2"

DB_URL = "postgresql://avnadmin:AVNS_WqF8fc_ApocbwKYmR9n@infigosdb-ifrs.j.aivencloud.com:16431/defaultdb?sslmode=require"

TABLE_MODEL_MAPPING = {
    "Run": Run,
    "Run_Input": RunInput,
    "Coverage_Units_Rec": CoverageUnitsRec,
    "Actual_Cashflow": CashFlow,
    "Liability_Init_Rec": Liability_Init_Rec,
    "Rec_Bel_Updated": Rec_Bel_Updated,
}
