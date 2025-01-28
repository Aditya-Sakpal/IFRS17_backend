import os
import traceback
from datetime import datetime
import pytz
import tempfile 
import uuid

from fastapi import APIRouter, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
import pandas as pd
from sqlalchemy import text

from db_related.connect import get_db,engine
from main_exec_func2 import calculate , IFRS4_calculate , GMMvsIFRS4
from models.calculation import Run , RunInput , CoverageUnitsRec , CashFlow , Liability_Init_Rec , Rec_Bel_Updated 

router = APIRouter()

@router.post('/insert_new_run')
async def insert_run(
    file: UploadFile = File(...),
    run_name: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, file.filename)

        with open(temp_file_path, "wb") as f:
            f.write(await file.read())

        df = pd.read_csv(temp_file_path)
        
        run_id = str(uuid.uuid4())
        conf_id = str(uuid.uuid4())
        reporting_date = datetime.now(pytz.utc).date()
        active_flag = True
        created_by = "Infogis_User"
        created_date = datetime.now(pytz.utc)
        modified_by = "Infogis_User"
        modified_date = datetime.now(pytz.utc)
        
        ifrs17_data = calculate(df)
        ifrs4_data = IFRS4_calculate(df)
        comp = GMMvsIFRS4(ifrs17_data,ifrs4_data)

        coverage_units_rec = ifrs17_data['Coverage Units Reconciliation']
        actual_cashflow = ifrs17_data['Actual Risk Adjustment CFs']
        liab_init_reco = ifrs17_data['Liability on Initial Recognition']
        rec_bel = ifrs17_data['Reconciliation of Best Estimate Liabilities']
        rec_ra = ifrs17_data['Reconciliation of Risk Adjustment']
        rec_csm = ifrs17_data['Reconciliation of Contractual Service Margin']
        rec_totcontliab_up = ifrs17_data['Reconciliation of Total Contract Liability']
        rec_acqexpmor_up = ifrs17_data['Reconciliation of Acquisition Expense Amortization']
        stat_profloss_up = ifrs17_data['Statement of Profit or Loss']
        
        run = pd.DataFrame({
            "Run_ID": [run_id],
            "Run_Name": [run_name],
            "Conf_ID": [conf_id],
            "Reporting_Date": [reporting_date],
            "Active_Flag": [active_flag],
            "Created_By": [created_by],
            "Created_Date": [created_date],
            "Modified_By": [modified_by],
            "Modified_Date": [modified_date]
        })
        coverage_units_rec['Coverage_Units_Rec_ID'] = [str(uuid.uuid4()) for _ in range(len(coverage_units_rec))] 
        coverage_units_rec['Run_ID'] = [run_id] * len(coverage_units_rec)
        coverage_units_rec['Active_Flag'] = [active_flag] * len(coverage_units_rec)
        coverage_units_rec['Created_By'] = [created_by] * len(coverage_units_rec)
        coverage_units_rec['Created_Date'] = [created_date] * len(coverage_units_rec)
        coverage_units_rec['Modified_By'] = [modified_by] * len(coverage_units_rec)
        coverage_units_rec['Modified_Date'] = [modified_date] * len(coverage_units_rec)
        
        actual_cashflow['Actual_Cashflow_ID'] = [str(uuid.uuid4()) for _ in range(len(actual_cashflow))]
        actual_cashflow['Run_ID'] = [run_id] * len(actual_cashflow)
        actual_cashflow['Active_Flag'] = [active_flag] * len(actual_cashflow)
        actual_cashflow['Created_By'] = [created_by] * len(actual_cashflow)
        actual_cashflow['Created_Date'] = [created_date] * len(actual_cashflow)
        actual_cashflow['Modified_By'] = [modified_by] * len(actual_cashflow)
        actual_cashflow['Modified_Date'] = [modified_date] * len(actual_cashflow)
        
        liab_init_reco['Liability_Init_Rec_ID'] = [str(uuid.uuid4()) for _ in range(len(liab_init_reco))]
        liab_init_reco['Run_ID'] = [run_id] * len(liab_init_reco)
        liab_init_reco['Active_Flag'] = [active_flag] * len(liab_init_reco)
        liab_init_reco['Created_By'] = [created_by] * len(liab_init_reco)
        liab_init_reco['Created_Date'] = [created_date] * len(liab_init_reco)
        liab_init_reco['Modified_By'] = [modified_by] * len(liab_init_reco)
        liab_init_reco['Modified_Date'] = [modified_date] * len(liab_init_reco)
        liab_init_reco.drop(['PV_DirExpen'],axis=1,inplace=True)
        
        rec_bel['Rec_BEL_ID'] = [str(uuid.uuid4()) for _ in range(len(rec_bel))]
        rec_bel['Run_ID'] = [run_id] * len(rec_bel)
        rec_bel['Active_Flag'] = [active_flag] * len(rec_bel)
        rec_bel['Created_By'] = [created_by] * len(rec_bel)
        rec_bel['Created_Date'] = [created_date] * len(rec_bel)
        rec_bel['Modified_By'] = [modified_by] * len(rec_bel)
        rec_bel['Modified_Date'] = [modified_date] * len(rec_bel)
        
        rec_ra['Rec_RA_ID'] = [str(uuid.uuid4()) for _ in range(len(rec_ra))]
        rec_ra['Run_ID'] = [run_id] * len(rec_ra)
        rec_ra['Active_Flag'] = [active_flag] * len(rec_ra)
        rec_ra['Created_By'] = [created_by] * len(rec_ra)
        rec_ra['Created_Date'] = [created_date] * len(rec_ra)
        rec_ra['Modified_By'] = [modified_by] * len(rec_ra)
        rec_ra['Modified_Date'] = [modified_date] * len(rec_ra)
        
        rec_csm['Rec_CSM_ID'] = [str(uuid.uuid4()) for _ in range(len(rec_csm))]
        rec_csm['Run_ID'] = [run_id] * len(rec_csm)
        rec_csm['Active_Flag'] = [active_flag] * len(rec_csm)
        rec_csm['Created_By'] = [created_by] * len(rec_csm)
        rec_csm['Created_Date'] = [created_date] * len(rec_csm)
        
        rec_totcontliab_up['Rec_TotContLiab_ID'] = [str(uuid.uuid4()) for _ in range(len(rec_totcontliab_up))]
        rec_totcontliab_up['Run_ID'] = [run_id] * len(rec_totcontliab_up)
        rec_totcontliab_up['Active_Flag'] = [active_flag] * len(rec_totcontliab_up)
        rec_totcontliab_up['Created_By'] = [created_by] * len(rec_totcontliab_up)
        rec_totcontliab_up['Created_Date'] = [created_date] * len(rec_totcontliab_up)
        rec_totcontliab_up['Modified_By'] = [modified_by] * len(rec_totcontliab_up)
        rec_totcontliab_up['Modified_Date'] = [modified_date] * len(rec_totcontliab_up)
        
        rec_acqexpmor_up['Rec_AcqExpMor_ID'] = [str(uuid.uuid4()) for _ in range(len(rec_acqexpmor_up))]
        rec_acqexpmor_up['Run_ID'] = [run_id] * len(rec_acqexpmor_up)
        rec_acqexpmor_up['Active_Flag'] = [active_flag] * len(rec_acqexpmor_up)
        rec_acqexpmor_up['Created_By'] = [created_by] * len(rec_acqexpmor_up)
        rec_acqexpmor_up['Created_Date'] = [created_date] * len(rec_acqexpmor_up)
        rec_acqexpmor_up['Modified_By'] = [modified_by] * len(rec_acqexpmor_up)
        rec_acqexpmor_up['Modified_Date'] = [modified_date] * len(rec_acqexpmor_up)
        
        stat_profloss_up['Stat_Profloss_ID'] = [str(uuid.uuid4()) for _ in range(len(stat_profloss_up))]
        stat_profloss_up['Run_ID'] = [run_id] * len(stat_profloss_up)
        stat_profloss_up['Active_Flag'] = [active_flag] * len(stat_profloss_up)
        stat_profloss_up['Created_By'] = [created_by] * len(stat_profloss_up)
        stat_profloss_up['Created_Date'] = [created_date] * len(stat_profloss_up)
        stat_profloss_up['Modified_By'] = [modified_by] * len(stat_profloss_up)
        stat_profloss_up['Modified_Date'] = [modified_date] * len(stat_profloss_up)
        
        
        IFRS4_Coverage_Units_Rec = ifrs4_data['Coverage Units Reconciliation']
        IFRS4_Actual_Cashflow = ifrs4_data['Actual Cashflows']
        IFRS4_Liability_Init_Rec = ifrs4_data['Liability on Initial Recognition']
        IFRS4_Rec_BEL = ifrs4_data['Reconciliation of Best Estimate Liabilities']
        IFRS4_Rec_AcqExpMor = ifrs4_data['Reconciliation of Acquisition Expense Amortization']
        IFRS4_Stat_Profloss = ifrs4_data['Statement of Profit or Loss']
        
        IFRS4_Coverage_Units_Rec['Coverage_Units_Rec_ID'] = [str(uuid.uuid4()) for _ in range(len(IFRS4_Coverage_Units_Rec))]
        IFRS4_Coverage_Units_Rec['Run_ID'] = [run_id] * len(IFRS4_Coverage_Units_Rec)
        IFRS4_Coverage_Units_Rec['Active_Flag'] = [active_flag] * len(IFRS4_Coverage_Units_Rec)
        IFRS4_Coverage_Units_Rec['Created_By'] = [created_by] * len(IFRS4_Coverage_Units_Rec)
        IFRS4_Coverage_Units_Rec['Created_Date'] = [created_date] * len(IFRS4_Coverage_Units_Rec)
        IFRS4_Coverage_Units_Rec['Modified_By'] = [modified_by] * len(IFRS4_Coverage_Units_Rec)
        IFRS4_Coverage_Units_Rec['Modified_Date'] = [modified_date] * len(IFRS4_Coverage_Units_Rec)
        
        IFRS4_Actual_Cashflow['Actual_Cashflow_ID'] = [str(uuid.uuid4()) for _ in range(len(IFRS4_Actual_Cashflow))]
        IFRS4_Actual_Cashflow['Run_ID'] = [run_id] * len(IFRS4_Actual_Cashflow)
        IFRS4_Actual_Cashflow['Active_Flag'] = [active_flag] * len(IFRS4_Actual_Cashflow)
        IFRS4_Actual_Cashflow['Created_By'] = [created_by] * len(IFRS4_Actual_Cashflow)
        IFRS4_Actual_Cashflow['Created_Date'] = [created_date] * len(IFRS4_Actual_Cashflow)
        IFRS4_Actual_Cashflow['Modified_By'] = [modified_by] * len(IFRS4_Actual_Cashflow)
        IFRS4_Actual_Cashflow['Modified_Date'] = [modified_date] * len(IFRS4_Actual_Cashflow)
        
        IFRS4_Liability_Init_Rec['Liability_Init_Rec_ID'] = [str(uuid.uuid4()) for _ in range(len(IFRS4_Liability_Init_Rec))]
        IFRS4_Liability_Init_Rec['Run_ID'] = [run_id] * len(IFRS4_Liability_Init_Rec)
        IFRS4_Liability_Init_Rec['Active_Flag'] = [active_flag] * len(IFRS4_Liability_Init_Rec)
        IFRS4_Liability_Init_Rec['Created_By'] = [created_by] * len(IFRS4_Liability_Init_Rec)
        IFRS4_Liability_Init_Rec['Created_Date'] = [created_date] * len(IFRS4_Liability_Init_Rec)
        IFRS4_Liability_Init_Rec['Modified_By'] = [modified_by] * len(IFRS4_Liability_Init_Rec)
        IFRS4_Liability_Init_Rec['Modified_Date'] = [modified_date] * len(IFRS4_Liability_Init_Rec)
        IFRS4_Liability_Init_Rec.drop(['PV_DirExpen'],axis=1,inplace=True)
        
        IFRS4_Rec_BEL['Rec_BEL_ID'] = [str(uuid.uuid4()) for _ in range(len(IFRS4_Rec_BEL))]
        IFRS4_Rec_BEL['Run_ID'] = [run_id] * len(IFRS4_Rec_BEL)
        IFRS4_Rec_BEL['Active_Flag'] = [active_flag] * len(IFRS4_Rec_BEL)
        IFRS4_Rec_BEL['Created_By'] = [created_by] * len(IFRS4_Rec_BEL)
        IFRS4_Rec_BEL['Created_Date'] = [created_date] * len(IFRS4_Rec_BEL)
        IFRS4_Rec_BEL['Modified_By'] = [modified_by] * len(IFRS4_Rec_BEL)
        IFRS4_Rec_BEL['Modified_Date'] = [modified_date] * len(IFRS4_Rec_BEL)
        
        IFRS4_Rec_AcqExpMor['Rec_AcqExpMor_ID'] = [str(uuid.uuid4()) for _ in range(len(IFRS4_Rec_AcqExpMor))]
        IFRS4_Rec_AcqExpMor['Run_ID'] = [run_id] * len(IFRS4_Rec_AcqExpMor)
        IFRS4_Rec_AcqExpMor['Active_Flag'] = [active_flag] * len(IFRS4_Rec_AcqExpMor)
        IFRS4_Rec_AcqExpMor['Created_By'] = [created_by] * len(IFRS4_Rec_AcqExpMor)
        IFRS4_Rec_AcqExpMor['Created_Date'] = [created_date] * len(IFRS4_Rec_AcqExpMor)
        IFRS4_Rec_AcqExpMor['Modified_By'] = [modified_by] * len(IFRS4_Rec_AcqExpMor)
        IFRS4_Rec_AcqExpMor['Modified_Date'] = [modified_date] * len(IFRS4_Rec_AcqExpMor)
        
        IFRS4_Stat_Profloss['Stat_Profloss_ID'] = [str(uuid.uuid4()) for _ in range(len(IFRS4_Stat_Profloss))]
        IFRS4_Stat_Profloss['Run_ID'] = [run_id] * len(IFRS4_Stat_Profloss)
        IFRS4_Stat_Profloss['Active_Flag'] = [active_flag] * len(IFRS4_Stat_Profloss)
        IFRS4_Stat_Profloss['Created_By'] = [created_by] * len(IFRS4_Stat_Profloss)
        IFRS4_Stat_Profloss['Created_Date'] = [created_date] * len(IFRS4_Stat_Profloss)
        IFRS4_Stat_Profloss['Modified_By'] = [modified_by] * len(IFRS4_Stat_Profloss)
        IFRS4_Stat_Profloss['Modified_Date'] = [modified_date] * len(IFRS4_Stat_Profloss)
        IFRS4_Stat_Profloss=IFRS4_Stat_Profloss.rename(columns={'Invest_inc':'Invest_Inc'})
        IFRS4_Stat_Profloss=IFRS4_Stat_Profloss.rename(columns={'Chnge_Insu_Liab':'Change_Ins_Liab'})
        
        with engine.begin() as conn:
            run.to_sql('Run',schema='Calculation',con=conn,if_exists='append',index=False)
            coverage_units_rec.to_sql('Coverage_Units_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            actual_cashflow.to_sql('Actual_Cashflow',schema='Calculation',con=conn,if_exists='append',index=False)
            liab_init_reco.to_sql('Liability_Init_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            rec_bel.to_sql('Rec_BEL',schema='Calculation',con=conn,if_exists='append',index=False)
            rec_ra.to_sql('Rec_RA',schema='Calculation',con=conn,if_exists='append',index=False)
            rec_csm.to_sql('Rec_CSM',schema='Calculation',con=conn,if_exists='append',index=False)
            rec_totcontliab_up.to_sql('Rec_TotContLiab',schema='Calculation',con=conn,if_exists='append',index=False)
            rec_acqexpmor_up.to_sql('Rec_AcqExpMor',schema='Calculation',con=conn,if_exists='append',index=False)
            stat_profloss_up.to_sql('Stat_Profloss',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Coverage_Units_Rec.to_sql('IFRS4_Coverage_Units_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Actual_Cashflow.to_sql('IFRS4_Actual_Cashflow',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Liability_Init_Rec.to_sql('IFRS4_Liability_Init_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Rec_BEL.to_sql('IFRS4_Rec_BEL',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Rec_AcqExpMor.to_sql('IFRS4_Rec_AcqExpMor',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Stat_Profloss.to_sql('IFRS4_Stat_Profloss',schema='Calculation',con=conn,if_exists='append',index=False)
        
        return JSONResponse(content={"data":comp,"run_id":run_id}, status_code=200)
        
    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}


@router.post('/_insert_new_run')
async def upload_csv(
    file: UploadFile = File(...),
    run_name: str = Form(...), 
    db: Session = Depends(get_db)
):
    try:
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, file.filename)

        with open(temp_file_path, "wb") as f:
            f.write(await file.read())

        df = pd.read_csv(temp_file_path)
        run , run_input,coverage_uni_recon, actual_cashflow, Liab_init_reco, Rec_BEL_updated, Rec_RA_updated, Rec_CSM_updated, Rec_TotContLiab_up, Rec_AcqExpMor_up, Stat_Profloss_up = calculate(df,run_name)
        data = IFRS4_calculate(df)
        
        run_input = run_input.rename(columns={
                                            'sum_assured':'Sum_Assured',
                                            'prem_rate_per1000':'Prem_Rate_Per1000',
                                            'policy_fees':'Policy_Fees',
                                            'acq_indirect_expense':'Acq_Indirect_Expense',
                                            'acq_direct_expenses':'Acq_Direct_Expenses',
                                            'main_direct_expenses':'Main_Direct_Expenses',
                                            'main_indirect_expenses':'Main_Indirect_Expenses',
                                            'discount_rate':'Discount_Rate',
                                            'asset_ret_rate':'Asset_Ret_Rate',
                                            'risk_adjst_rate':'Risk_Adjst_Rate',
                                            'mortality':'Mortality',
                                            'lapse':'Lapse',
                                            'policy_yearly_comm':'Policy_Yrly_Com',
                                            'policy_Init_comm':'Policy_Init_Com',
                                            'Total_years':'Total_Years',
                                            'CSM_ret_Rate':'CSM_Ret_Rate',
                                            })
        
        IFRS4_Coverage_Units_Rec = data['Coverage Units Reconciliation']
        IFRS4_Coverage_Units_Rec['Coverage_Units_Rec_ID'] = coverage_uni_recon['Coverage_Units_Rec_ID']
        IFRS4_Coverage_Units_Rec['Run_ID'] = run['Run_ID']
        IFRS4_Coverage_Units_Rec['Created_By'] = run['Created_By']
        IFRS4_Coverage_Units_Rec['Created_Date'] = run['Created_Date']
        IFRS4_Coverage_Units_Rec['Modified_By'] = run['Modified_By']
        IFRS4_Coverage_Units_Rec['Modified_Date'] = run['Modified_Date']
        
        IFRS4_Liability_Init_Rec=data['Liability on Initial Recognition']
        IFRS4_Liability_Init_Rec['Liability_Init_Rec_ID'] = Liab_init_reco['Liability_Init_Rec_ID']
        IFRS4_Liability_Init_Rec['Run_ID'] = run['Run_ID']
        IFRS4_Liability_Init_Rec['Created_By'] = run['Created_By']
        IFRS4_Liability_Init_Rec['Created_Date'] = run['Created_Date']
        IFRS4_Liability_Init_Rec['Modified_By'] = run['Modified_By']
        IFRS4_Liability_Init_Rec['Modified_Date'] = run['Modified_Date']
        IFRS4_Liability_Init_Rec.drop(['PV_DirExpen'],axis=1,inplace=True)
        
        IFRS4_Rec_BEL=data['Reconciliation of Best Estimate Liabilities']
        IFRS4_Rec_BEL['Rec_BEL_ID'] = Rec_BEL_updated['Rec_BEL_ID']
        IFRS4_Rec_BEL['Run_ID'] = run['Run_ID']
        IFRS4_Rec_BEL['Created_By'] = run['Created_By']
        IFRS4_Rec_BEL['Created_Date'] = run['Created_Date']
        IFRS4_Rec_BEL['Modified_By'] = run['Modified_By']
        IFRS4_Rec_BEL['Modified_Date'] = run['Modified_Date']
        
        IFRS4_Rec_AcqExpMor = data['Reconciliation of Acquisition Expense Amortization']
        IFRS4_Rec_AcqExpMor['Rec_AcqExpMor_ID'] = Rec_AcqExpMor_up['Rec_AcqExpMor_ID']
        IFRS4_Rec_AcqExpMor['Run_ID'] = run['Run_ID']
        IFRS4_Rec_AcqExpMor['Created_By'] = run['Created_By']
        IFRS4_Rec_AcqExpMor['Created_Date'] = run['Created_Date']
        IFRS4_Rec_AcqExpMor['Modified_By'] = run['Modified_By']
        IFRS4_Rec_AcqExpMor['Modified_Date'] = run['Modified_Date']
        
        IFRS4_Actual_Cashflow = data['Actual Cashflows']
        IFRS4_Actual_Cashflow['Actual_Cashflow_ID'] = actual_cashflow['Actual_Cashflow_ID']
        IFRS4_Actual_Cashflow['Run_ID'] = run['Run_ID']
        IFRS4_Actual_Cashflow['Created_By'] = run['Created_By']
        IFRS4_Actual_Cashflow['Created_Date'] = run['Created_Date']
        IFRS4_Actual_Cashflow['Modified_By'] = run['Modified_By']
        IFRS4_Actual_Cashflow['Modified_Date'] = run['Modified_Date']
        
        IFRS4_Stat_Profloss = data['Statement of Profit or Loss']
        IFRS4_Stat_Profloss['Stat_Profloss_ID'] = Stat_Profloss_up['Stat_Profloss_ID']
        IFRS4_Stat_Profloss['Run_ID'] = run['Run_ID']
        IFRS4_Stat_Profloss['Created_By'] = run['Created_By']
        IFRS4_Stat_Profloss['Created_Date'] = run['Created_Date']
        IFRS4_Stat_Profloss['Modified_By'] = run['Modified_By']
        IFRS4_Stat_Profloss['Modified_Date'] = run['Modified_Date']
        IFRS4_Stat_Profloss=IFRS4_Stat_Profloss.rename(columns={'Invest_inc':'Invest_Inc'})
        IFRS4_Stat_Profloss=IFRS4_Stat_Profloss.rename(columns={'Chnge_Insu_Liab':'Change_Ins_Liab'})
        
        run_input.drop(['Unnamed: 0'],axis=1,inplace=True)
        
        
        with engine.begin() as conn:
            run.to_sql('Run',schema='Calculation',con=conn,if_exists='append',index=False)
            run_input.to_sql('Run_Input',schema='Calculation',con=conn,if_exists='append',index=False)
            coverage_uni_recon.to_sql('Coverage_Units_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            actual_cashflow.to_sql('Actual_Cashflow',schema='Calculation',con=conn,if_exists='append',index=False)
            Liab_init_reco.to_sql('Liability_Init_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            Rec_BEL_updated.to_sql('Rec_BEL',schema='Calculation',con=conn,if_exists='append',index=False)
            Rec_RA_updated.to_sql('Rec_RA',schema='Calculation',con=conn,if_exists='append',index=False)
            Rec_CSM_updated.to_sql('Rec_CSM',schema='Calculation',con=conn,if_exists='append',index=False)
            Rec_TotContLiab_up.to_sql('Rec_TotContLiab',schema='Calculation',con=conn,if_exists='append',index=False)
            Rec_AcqExpMor_up.to_sql('Rec_AcqExpMor',schema='Calculation',con=conn,if_exists='append',index=False)
            Stat_Profloss_up.to_sql('Stat_Profloss',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Coverage_Units_Rec.to_sql('IFRS4_Coverage_Units_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Liability_Init_Rec.to_sql('IFRS4_Liability_Init_Rec',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Rec_BEL.to_sql('IFRS4_Rec_BEL',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Rec_AcqExpMor.to_sql('IFRS4_Rec_AcqExpMor',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Actual_Cashflow.to_sql('IFRS4_Actual_Cashflow',schema='Calculation',con=conn,if_exists='append',index=False)
            IFRS4_Stat_Profloss.to_sql('IFRS4_Stat_Profloss',schema='Calculation',con=conn,if_exists='append',index=False)
            
            
        os.remove(temp_file_path)

        return {"message": "CSV file processed successfully.", "run_name": run_name, "rows": len(df)}
    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    
@router.post("/get_gmm_vs_paa_diff", status_code=200)
def get_gmm_vs_paa_diff(
    run_id: str,
    table_name: str,
    db: Session = Depends(get_db)
):
    try:
        gmm_query = text(f'SELECT * FROM "Calculation"."{table_name}" WHERE "Run_ID" = :run_id')
        paas_query = text(f'SELECT * FROM "Calculation"."IFRS4_{table_name}" WHERE "Run_ID" = :run_id')
        
        gmm_result_set = db.execute(gmm_query, {"run_id": run_id})
        paas_result_set = db.execute(paas_query, {"run_id": run_id})
        
        gmm_column_names = [col[0] for col in gmm_result_set.cursor.description]
        paas_column_names = [col[0] for col in paas_result_set.cursor.description]
        
        gmm_data = gmm_result_set.fetchall()
        paas_data = paas_result_set.fetchall()
        
        gmm_df = pd.DataFrame(gmm_data, columns=gmm_column_names)
        paas_df = pd.DataFrame(paas_data, columns=paas_column_names)
        
        gmm_df = gmm_df.drop(['Run_ID', 'Active_Flag', 'Created_By', 'Created_Date', 'Modified_By', 'Modified_Date'], axis=1)
        paas_df = paas_df.drop(['Run_ID', 'Active_Flag', 'Created_By', 'Created_Date', 'Modified_By', 'Modified_Date'], axis=1)
        
        diff = GMMvsIFRS4(gmm_df, paas_df)
        
        return {"diff": diff.to_dict(orient='records')}
        
    except Exception as e:
        print(traceback.format_exc())
        return {"message": "Error in fetching data", "error": str(e)}

    
@router.get("/get_session_history", status_code=200)
def get_session_history(db: Session = Depends(get_db)):
    try:
        # Get today's date in UTC
        today = datetime.now(pytz.utc).date()

        # Query the Run table for records with Reporting_Date matching today's date
        sessions = db.query(Run).filter(Run.Reporting_Date == today).all()
        
        # Convert the result to a simplified dictionary format
        session_list = [
            {
                "Run_ID": session.Run_ID,
                "Run_Name": session.Run_Name,
                "Reporting_Date": session.Reporting_Date,
            }
            for session in sessions
        ]

        if not session_list:
            return {"message": "No sessions found for today's date"}

        return {"sessions": session_list}

    except Exception as e:
        print(traceback.format_exc())
        return {"message": "Error in fetching session history", "error": str(e)}
    
@router.get("/get_calculation_history", status_code=200)
def get_all_sessions(db: Session = Depends(get_db)):
    try:
        # Tables to fetch data from
        tables = [
            "Run",
            "Coverage_Units_Rec",
            "Actual_Cashflow",
            "Liability_Init_Rec",
            "Rec_BEL",
            "Rec_RA",
            "Rec_CSM",
            "Rec_TotContLiab",
            "Rec_AcqExpMor",
            "Stat_Profloss",
            'IFRS4_Coverage_Units_Rec',
            'IFRS4_Actual_Cashflow',
            'IFRS4_Liability_Init_Rec',
            'IFRS4_Stat_Profloss',
            'IFRS4_Rec_BEL',
            'IFRS4_Rec_AcqExpMor',
        ]

        # Dictionary to hold the results
        results = {}

        # Iterate over tables and fetch all records
        for table in tables:
            query = text(f'SELECT * FROM "Calculation"."{table}"')
            result_set = db.execute(query)

            # Extract column names from the description
            column_names = [col[0] for col in result_set.cursor.description]

            # Fetch all data and convert to list of dictionaries
            data = result_set.fetchall()
            results[table] = [dict(zip(column_names, row)) for row in data]

        if all(len(records) == 0 for records in results.values()):
            return {"message": "No records found in any table"}

        return {"calculationHistory": results}

    except Exception as e:
        print(traceback.format_exc())
        return {"message": "Error in fetching calculation history", "error": str(e)}