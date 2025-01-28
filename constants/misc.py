from models.calculation import Run, CoverageUnitsRec,CashFlow,Liability_Init_Rec,Rec_Bel_Updated,RunInput


# DB_URL="postgresql://infigos_solutions_user:U640zVayfFvOTg4S6U7hN14dy2UoVHBS@dpg-ctiudvdumphs73f70t90-a.singapore-postgres.render.com/infigos_solutions"

DB_URL = "postgresql://user:SLyfZfXToOaFPO7w6RhH0EIftTdl9ZjU@dpg-cucgmkrv2p9s73d7k9s0-a.singapore-postgres.render.com/infigos_db_2"
# DB_URL = "postgresql://postgres:postgres@localhost:5432/infigos_db_2"

TABLE_MODEL_MAPPING = {
    "Run": Run,
    "Run_Input": RunInput,
    "Coverage_Units_Rec": CoverageUnitsRec,
    "Actual_Cashflow": CashFlow,
    "Liability_Init_Rec": Liability_Init_Rec,
    "Rec_Bel_Updated": Rec_Bel_Updated,
}