from datetime import datetime
import uuid
import time

import pandas as pd
import numpy as np
from numpy_financial import npv

from db_related.records_insertion import send_bulk_records

def calculate(df_temp):

    sum_assured = df_temp['sum_assured'].astype(float).item()
    Num_Policies = df_temp['Num_Policies'].astype(float).item()
    policy_fees = df_temp['policy_fees'].astype(int).item()
    prem_rate_per1000 = df_temp['prem_rate_per1000'].astype(float).item()
    policy_Init_comm = df_temp['policy_Init_comm'].astype(float).item()
    policy_yearly_comm = df_temp['policy_yearly_comm'].astype(float).item()
    acq_direct_expenses = df_temp['acq_direct_expenses'].astype(float).item()
    acq_indirect_expense = df_temp['acq_indirect_expense'].astype(float).item()
    main_direct_expenses = df_temp['main_direct_expenses'].astype(float).item()
    main_indirect_expenses = df_temp['main_indirect_expenses'].astype(float).item()
    Total_years = df_temp['Total_years'].astype(int).item()
    discount_rate = df_temp['discount_rate'].astype(float).item()
    asset_ret_rate = df_temp['asset_ret_rate'].astype(float).item()
    CSM_ret_Rate = df_temp['CSM_ret_Rate'].astype(float).item()
    risk_adjst_rate = df_temp['risk_adjst_rate'].astype(float).item()
    mortality = df_temp['mortality'].astype(float).item()
    lapse = df_temp['lapse'].astype(int).item()
    
    # Create a DataFrame from the user input
    Opening = [0]*Total_years
    Deaths = [0]*Total_years
    Lapses = [0]*Total_years
    Closes = [0]*Total_years
    
    coverage_uni_recon = pd.DataFrame({
        'Opening': Opening,
        'Deaths': Deaths,
        'Lapses': lapse,
        'Closes': Closes
    })
    
    Premiums = [0]*Total_years
    Acquisition_Comm = [0]*Total_years
    Renewal_Comm = [0]*Total_years
    Acq_Exp_Attr = [0]*Total_years
    Maint_Exp_Attr = [0]*Total_years
    Acq_Exp_N_Attr = [0]*Total_years
    Maint_Exp_N_Attr = [0]*Total_years
    Claims = [0]*Total_years
    Total_Net_CFs = [0]*Total_years
    
    actual_cashflow = pd.DataFrame(
        {'Premiums': Premiums,
         'Acquisition_Comm': Acquisition_Comm,
         'Renewal_Comm': Renewal_Comm,
         'Acq_Exp_Attr': Acq_Exp_Attr,
         'Maint_Exp_Attr': Maint_Exp_Attr,
         'Acq_Exp_N_Attr': Acq_Exp_N_Attr,
         'Maint_Exp_N_Attr': Maint_Exp_N_Attr,
         'Claims': Claims,
         'Total_Net_CFs': Total_Net_CFs
         })
    
    Opening = [0]*Total_years
    NewBusiness = [0]*Total_years
    Assump = [0]*Total_years
    ExpInflow = [0]*Total_years
    ExpOutFlow = [0]*Total_years
    FinExp = [0]*Total_years
    Changes_Exp = [0]*Total_years
    Changes_Rel = [0]*Total_years
    Closes = [0]*Total_years
    Loss_Comp = [0]*Total_years
    
    Rec_BEL = pd.DataFrame(
        {'Opening': Opening,
         'NewBusiness': NewBusiness,
         'Assump': Assump,
         'ExpInflow': ExpInflow,
         'ExpOutFlow': ExpOutFlow,
         'FinExp': FinExp,
         'Changes_Exp': Changes_Exp,
         'Changes_Rel': Changes_Rel,
         'Closes': Closes
         })
    
    Rec_RA = pd.DataFrame(
        {'Opening': Opening,
         'NewBusiness': NewBusiness,
         'Assump': Assump,
         'ExpInflow': ExpInflow,
         'ExpOutFlow': ExpOutFlow,
         'FinExp': FinExp,
         'Changes_Exp': Changes_Exp,
         'Changes_Rel': Changes_Rel,
         'Closes': Closes
         })
    
    Rec_CSM = pd.DataFrame(
        {'Opening': Opening,
         'NewBusiness': NewBusiness,
         'Assump': Assump,
         'ExpInflow': ExpInflow,
         'ExpOutFlow': ExpOutFlow,
         'FinExp': FinExp,
         'Changes_Exp': Changes_Exp,
         'Changes_Rel': Changes_Rel,
         'Closes': Closes
         })
    
    Rec_TotContLiab = pd.DataFrame(
        {'Opening': Opening,
         'NewBusiness': NewBusiness,
         'Assump': Assump,
         'ExpInflow': ExpInflow,
         'ExpOutFlow': ExpOutFlow,
         'FinExp': FinExp,
         'Changes_Exp': Changes_Exp,
         'Changes_Rel': Changes_Rel,
         'Closes': Closes
         })
    
    Opening = [0]*Total_years
    NewAcqExp = [0]*Total_years
    AccIntr = [0]*Total_years
    AmorExp = [0]*Total_years
    Closes = [0]*Total_years
    
    Rec_AcqExpMor = pd.DataFrame(
        {'Opening': Opening,
         'NewAcqExp': NewAcqExp,
         'AccIntr': AccIntr,
         'AmorExp': AmorExp,
         'Closes': Closes
         })
    
    Rel_CSM = [0]*Total_years
    Rel_RA = [0]*Total_years
    Exp_Claim = [0]*Total_years
    Exp_Expen = [0]*Total_years
    Rec_AcqCasFl = [0]*Total_years
    Ins_SerRev = [0]*Total_years
    Claim_Incur = [0]*Total_years
    Exp_Incur = [0]*Total_years
    Amor_AcqCasFlo = [0]*Total_years
    Ins_SerExp = [0]*Total_years
    Other_Exp = [0]*Total_years
    Inv_income = [0]*Total_years
    Ins_FinExp = [0]*Total_years
    Fin_GainLoss = [0]*Total_years
    
    Prof_loss = [0]*Total_years
    
    Stat_Profloss = pd.DataFrame(
        {'Rel_CSM': Rel_CSM,
         'Rel_RA': Rel_RA,
         'Exp_Claim': Exp_Claim,
         'Exp_Expen': Exp_Expen,
         'Rec_AcqCasFl': Rec_AcqCasFl,
         'Ins_SerRev': Ins_SerRev,
         
         'Claim_Incur': Claim_Incur,
         'Exp_Incur': Exp_Incur,
         'Amor_AcqCasFlo': Amor_AcqCasFlo,
         'Ins_SerExp': Ins_SerExp,
         
         'Other_Exp': Other_Exp,
         
         'Inv_income': Exp_Incur,
         'Ins_FinExp': Amor_AcqCasFlo,
         'Fin_GainLoss': Ins_SerExp,
         'Prof_loss': Ins_SerExp,
         
         })
    
    # Coverage units reconcilation
    coverage_uni_recon_updated = coverage_uni_recon.copy()
    temp = Num_Policies
    
    for i, row in coverage_uni_recon_updated.iterrows():
        coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]] = temp
        
        coverage_uni_recon_updated['Deaths'].loc[coverage_uni_recon_updated.index[i]] = mortality * \
            coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]/100
        
        coverage_uni_recon_updated['Lapses'].loc[coverage_uni_recon_updated.index[i]] = lapse * \
            coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]/100
        
        coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] = (coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
                                                                                          - coverage_uni_recon_updated['Deaths'].loc[coverage_uni_recon_updated.index[i]]
                                                                                          - coverage_uni_recon_updated['Lapses'].loc[coverage_uni_recon_updated.index[i]])
        if i < len(coverage_uni_recon):
            temp = coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]]
        
        if (coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] < 0):
            coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] = 0
    
    coverage_uni_recon_updated_T = coverage_uni_recon_updated.T
    # print(coverage_uni_recon_updated.T)
    
    # cash flow calculations
    actual_cashflow_updated = actual_cashflow.copy()
    
    for i, row in actual_cashflow_updated.iterrows():
        
        # 1
        actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]] = (
            sum_assured * prem_rate_per1000/1000 + policy_fees) * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
        # 2
        if i == 0:
            actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]] = - policy_Init_comm * actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]]
        else:
            actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]] = 0
        # 3
        if i == 0:
            actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]] = 0
        else:
            actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]] = - policy_yearly_comm * actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]]
        # 4
        if i == 0:
            actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]] = - acq_direct_expenses * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
        else:
            actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]] = 0
        # 5
        actual_cashflow_updated['Maint_Exp_Attr'].loc[actual_cashflow_updated.index[i]] = - main_direct_expenses * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
        # 6
        if i == 0:
            actual_cashflow_updated['Acq_Exp_N_Attr'].loc[actual_cashflow_updated.index[i]] = - acq_indirect_expense * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
        else:
            actual_cashflow_updated['Acq_Exp_N_Attr'].loc[actual_cashflow_updated.index[i]] = 0
        # 7
        actual_cashflow_updated['Maint_Exp_N_Attr'].loc[actual_cashflow_updated.index[i]] = - main_indirect_expenses * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
        # 8
        actual_cashflow_updated['Claims'].loc[actual_cashflow_updated.index[i]] = -sum_assured * coverage_uni_recon_updated['Deaths'].loc[coverage_uni_recon_updated.index[i]]
        # 9
        actual_cashflow_updated['Total_Net_CFs'].loc[actual_cashflow_updated.index[i]] = (actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]]
                                                                                          + actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                                          + actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                                          + actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                                          + actual_cashflow_updated['Maint_Exp_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                                          + actual_cashflow_updated['Acq_Exp_N_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                                          + actual_cashflow_updated['Maint_Exp_N_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                                          + actual_cashflow_updated['Claims'].loc[actual_cashflow_updated.index[i]]
                                                                                          )
    
    risk_adjust = actual_cashflow_updated['Claims'] * risk_adjst_rate
    
    PV_Prem = npv(discount_rate, actual_cashflow_updated['Premiums'])
    PV_RenComm = npv(discount_rate, actual_cashflow_updated['Renewal_Comm'])
    PV_DirExpen = (actual_cashflow_updated['Maint_Exp_Attr'] / (1 + discount_rate) ** np.arange(
        1, len(actual_cashflow_updated['Maint_Exp_Attr']) + 1)).sum(axis=0)
    PV_Claims = (actual_cashflow_updated['Claims'] / (1 + discount_rate) ** np.arange(
        1, len(actual_cashflow_updated['Claims']) + 1)).sum(axis=0)
    PV_DirAqis = (actual_cashflow_updated['Acquisition_Comm'].values +
                  actual_cashflow_updated['Acq_Exp_Attr'].values)[0]
    PV_RiskAdj = (risk_adjust / (1 + discount_rate) **
                  np.arange(1, len(risk_adjust) + 1)).sum(axis=0)
    Total = PV_Prem + PV_RenComm + PV_DirExpen + PV_Claims + PV_DirAqis + PV_RiskAdj
    CSM_Init = Total
    Liab_Init = 0
    Liab_init_reco = pd.DataFrame({
        'PV_Prem': PV_Prem,
        'PV_RenComm': PV_RenComm,
        'PV_DirExpen': PV_DirExpen,
        'PV_Claims': PV_Claims,
        'PV_DirAqis': PV_DirAqis,
        'PV_RiskAdj': PV_RiskAdj,
        'Total': Total,
        'CSM_Init': CSM_Init,
        'Liab_Init': Liab_Init}, index=[0])
    
    Rec_BEL_updated = Rec_BEL.copy()
    temp = 0
    for i, row in Rec_BEL_updated.iterrows():
        # 1
        Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]] = temp
        # 2
        if i == 0:
            Rec_BEL_updated['NewBusiness'].loc[Rec_BEL_updated.index[i]] = - (PV_Prem + PV_RenComm + PV_DirExpen + PV_Claims + PV_DirAqis)
        else:
            Rec_BEL_updated['NewBusiness'].loc[Rec_BEL_updated.index[i]] = 0
        # 3
        Rec_BEL_updated['Assump'].loc[Rec_BEL_updated.index[i]] = 0
        # 4
        Rec_BEL_updated['ExpInflow'].loc[Rec_BEL_updated.index[i]] = actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]]
        # 5
        Rec_BEL_updated['ExpOutFlow'].loc[Rec_BEL_updated.index[i]] = (actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Maint_Exp_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Claims'].loc[actual_cashflow_updated.index[i]]
                                                                       )
        # 6
        Rec_BEL_updated['FinExp'].loc[Rec_BEL_updated.index[i]] = discount_rate * (Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]] +
                                                                                   Rec_BEL_updated['NewBusiness'].loc[Rec_BEL_updated.index[i]]) + discount_rate * (actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]] +
                                                                                                                                                                    + actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                                                                                                                    + actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                                                                                                                    + actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]])
        # 7
        Rec_BEL_updated['Changes_Exp'].loc[Rec_BEL_updated.index[i]] = 0
        # 8
        Rec_BEL_updated['Changes_Rel'].loc[Rec_BEL_updated.index[i]] = 0
        # 9
        Rec_BEL_updated['Closes'].loc[Rec_BEL_updated.index[i]] = (Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['NewBusiness'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['Assump'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['ExpInflow'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['ExpOutFlow'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['FinExp'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['Changes_Exp'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['Changes_Rel'].loc[Rec_BEL_updated.index[i]])
        temp = Rec_BEL_updated['Closes'].loc[Rec_BEL_updated.index[i]]

    Rec_RA_updated = Rec_RA.copy()
    temp = 0
    for i, row in Rec_RA_updated.iterrows():
        # 1
        Rec_RA_updated['Opening'].loc[Rec_RA_updated.index[i]] = temp
        # 2
        if i == 0:
            Rec_RA_updated['NewBusiness'].loc[Rec_RA_updated.index[i]] = -PV_RiskAdj
        else:
            Rec_RA_updated['NewBusiness'].loc[Rec_RA_updated.index[i]] = 0
        # 3
        Rec_RA_updated['Assump'].loc[Rec_RA_updated.index[i]] = 0
        # 4
        Rec_RA_updated['ExpInflow'].loc[Rec_RA_updated.index[i]] = 0
        # 5
        Rec_RA_updated['ExpOutFlow'].loc[Rec_RA_updated.index[i]] = 0
        # 6
        Rec_RA_updated['FinExp'].loc[Rec_RA_updated.index[i]] = discount_rate * (Rec_RA_updated['Opening'].loc[Rec_RA_updated.index[i]] +
                                                                                 Rec_RA_updated['NewBusiness'].loc[Rec_RA_updated.index[i]])
        # 7
        Rec_RA_updated['Changes_Exp'].loc[Rec_RA_updated.index[i]] = 0
        # 8
        Rec_RA_updated['Changes_Rel'].loc[Rec_RA_updated.index[i]] = risk_adjust[i]
        # 9
        Rec_RA_updated['Closes'].loc[Rec_RA_updated.index[i]] = (Rec_RA_updated['Opening'].loc[Rec_RA_updated.index[i]] +
                                                                  Rec_RA_updated['NewBusiness'].loc[Rec_RA_updated.index[i]] +
                                                                  Rec_RA_updated['Assump'].loc[Rec_RA_updated.index[i]] +
                                                                  Rec_RA_updated['ExpInflow'].loc[Rec_RA_updated.index[i]] +
                                                                  Rec_RA_updated['ExpOutFlow'].loc[Rec_RA_updated.index[i]] +
                                                                  Rec_RA_updated['FinExp'].loc[Rec_RA_updated.index[i]] +
                                                                  Rec_RA_updated['Changes_Exp'].loc[Rec_RA_updated.index[i]] +
                                                                  Rec_RA_updated['Changes_Rel'].loc[Rec_RA_updated.index[i]])
        temp = Rec_RA_updated['Closes'].loc[Rec_RA_updated.index[i]]

    if CSM_Init > 0:
        Rec_CSM_updated = Rec_CSM.copy()
    else:
        Rec_CSM_updated = pd.DataFrame(
            {'Opening': Opening,
             'NewBusiness': NewBusiness,
             'Assump': Assump,
             'ExpInflow': ExpInflow,
             'ExpOutFlow': ExpOutFlow,
             'FinExp': FinExp,
             'Changes_Exp': Changes_Exp,
             'Changes_Rel': Changes_Rel,
             'Closes': Closes,
             'Loss_Comp': Loss_Comp
             
             })
    
    temp = 0
    loss_comp = False
    
    for i, row in Rec_CSM_updated.iterrows():
        # 1
        Rec_CSM_updated['Opening'].loc[Rec_CSM_updated.index[i]] = temp
        # 2
        if i == 0 and CSM_Init < 0:
            Rec_CSM_updated['NewBusiness'].loc[Rec_CSM_updated.index[i]] = CSM_Init
            #Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]] = CSM_Init
            loss_comp = True
        elif i == 0 and CSM_Init > 0:
            Rec_CSM_updated['NewBusiness'].loc[Rec_CSM_updated.index[i]] = CSM_Init
            loss_comp = False
        else:
            Rec_CSM_updated['NewBusiness'].loc[Rec_CSM_updated.index[i]] = 0
        # 3
        Rec_CSM_updated['Assump'].loc[Rec_CSM_updated.index[i]] = 0
        # 4
        Rec_CSM_updated['ExpInflow'].loc[Rec_CSM_updated.index[i]] = 0
        # 5
        Rec_CSM_updated['ExpOutFlow'].loc[Rec_CSM_updated.index[i]] = 0
        # 6
        Rec_CSM_updated['FinExp'].loc[Rec_CSM_updated.index[i]] = CSM_ret_Rate * (Rec_CSM_updated['Opening'].loc[Rec_CSM_updated.index[i]] +
                                                                                  Rec_CSM_updated['NewBusiness'].loc[Rec_CSM_updated.index[i]])
        # 7
        Rec_CSM_updated['Changes_Exp'].loc[Rec_CSM_updated.index[i]] = 0
        # 8
        Changes_Rel_denom = (Rec_CSM_updated['Opening'].loc[Rec_CSM_updated.index[i]] + Rec_CSM_updated['NewBusiness'].loc[Rec_CSM_updated.index[i]]
                            + Rec_CSM_updated['Assump'].loc[Rec_CSM_updated.index[i]] +
                            Rec_CSM_updated['ExpInflow'].loc[Rec_CSM_updated.index[i]]
                            + Rec_CSM_updated['ExpOutFlow'].loc[Rec_CSM_updated.index[i]] + Rec_CSM_updated['FinExp'].loc[Rec_CSM_updated.index[i]])
        sum_coverageUni = 0
        for j in range(i, Total_years):
            sum_coverageUni = sum_coverageUni + \
                coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[j]]
        
        Rec_CSM_updated['Changes_Rel'].loc[Rec_CSM_updated.index[i]] = -((Changes_Rel_denom * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]])/ sum_coverageUni)

        Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]] = (Rec_CSM_updated['Opening'].loc[Rec_CSM_updated.index[i]] +
                                                                    Rec_CSM_updated['NewBusiness'].loc[Rec_CSM_updated.index[i]] +
                                                                    Rec_CSM_updated['Assump'].loc[Rec_CSM_updated.index[i]] +
                                                                    Rec_CSM_updated['ExpInflow'].loc[Rec_CSM_updated.index[i]] +
                                                                    Rec_CSM_updated['ExpOutFlow'].loc[Rec_CSM_updated.index[i]] +
                                                                    Rec_CSM_updated['FinExp'].loc[Rec_CSM_updated.index[i]] +
                                                                    Rec_CSM_updated['Changes_Exp'].loc[Rec_CSM_updated.index[i]] +
                                                                    Rec_CSM_updated['Changes_Rel'].loc[Rec_CSM_updated.index[i]])
        if loss_comp == True and -Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]] > Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]]:
            Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]] = (Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]] +
                                                                          Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]])
            Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]] = 0
            temp = Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]]
        elif loss_comp == True and -Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]] <= Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]]:
            Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]] = (Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]] +
                                                                        Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]])
            Rec_CSM_updated['Loss_Comp'].loc[Rec_CSM_updated.index[i]] = 0
            temp = Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]]
        else:
            temp = Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]]
    Rec_CSM_updated_T = Rec_CSM_updated.T
    
    Rec_TotContLiab_up = Rec_TotContLiab.copy()
    for i, row in Rec_TotContLiab_up.iterrows():
        # 1
        Rec_TotContLiab_up['Opening'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]]
                                                                          + Rec_RA_updated['Opening'].loc[Rec_RA_updated.index[i]]
                                                                          + Rec_CSM_updated['Opening'].loc[Rec_CSM_updated.index[i]])
        # 2
        Rec_TotContLiab_up['NewBusiness'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['NewBusiness'].loc[Rec_BEL_updated.index[i]]
                                                                              + Rec_RA_updated['NewBusiness'].loc[Rec_RA_updated.index[i]]
                                                                              + Rec_CSM_updated['NewBusiness'].loc[Rec_CSM_updated.index[i]])
        # 3
        Rec_TotContLiab_up['Assump'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['Assump'].loc[Rec_BEL_updated.index[i]]
                                                                         + Rec_RA_updated['Assump'].loc[Rec_RA_updated.index[i]]
                                                                         + Rec_CSM_updated['Assump'].loc[Rec_CSM_updated.index[i]])
        # 4
        Rec_TotContLiab_up['ExpInflow'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['ExpInflow'].loc[Rec_BEL_updated.index[i]]
                                                                            + Rec_RA_updated['ExpInflow'].loc[Rec_RA_updated.index[i]]
                                                                            + Rec_CSM_updated['ExpInflow'].loc[Rec_CSM_updated.index[i]])
        # 5
        Rec_TotContLiab_up['ExpOutFlow'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['ExpOutFlow'].loc[Rec_BEL_updated.index[i]]
                                                                             + Rec_RA_updated['ExpOutFlow'].loc[Rec_RA_updated.index[i]]
                                                                             + Rec_CSM_updated['ExpOutFlow'].loc[Rec_CSM_updated.index[i]])
        # 6
        Rec_TotContLiab_up['FinExp'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['FinExp'].loc[Rec_BEL_updated.index[i]]
                                                                         + Rec_RA_updated['FinExp'].loc[Rec_RA_updated.index[i]]
                                                                         + Rec_CSM_updated['FinExp'].loc[Rec_CSM_updated.index[i]])
        # 7
        Rec_TotContLiab_up['Changes_Exp'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['Changes_Exp'].loc[Rec_BEL_updated.index[i]]
                                                                             + Rec_RA_updated['Changes_Exp'].loc[Rec_RA_updated.index[i]]
                                                                             + Rec_CSM_updated['Changes_Exp'].loc[Rec_CSM_updated.index[i]])
        # 8
        Rec_TotContLiab_up['Changes_Rel'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['Changes_Rel'].loc[Rec_BEL_updated.index[i]]
                                                                             + Rec_RA_updated['Changes_Rel'].loc[Rec_RA_updated.index[i]]
                                                                             + Rec_CSM_updated['Changes_Rel'].loc[Rec_CSM_updated.index[i]])
        # 9
        Rec_TotContLiab_up['Closes'].loc[Rec_TotContLiab_up.index[i]] = (Rec_BEL_updated['Closes'].loc[Rec_BEL_updated.index[i]]
                                                                          + Rec_RA_updated['Closes'].loc[Rec_RA_updated.index[i]]
                                                                          + Rec_CSM_updated['Closes'].loc[Rec_CSM_updated.index[i]])

    Rec_AcqExpMor_up = Rec_AcqExpMor.copy()
    temp = 0
    for i, row in Rec_AcqExpMor_up.iterrows():
        # 1
        Rec_AcqExpMor_up['Opening'].loc[Rec_AcqExpMor_up.index[i]] = temp
        # 2
        Rec_AcqExpMor_up['NewAcqExp'].loc[Rec_AcqExpMor_up.index[i]] = -(actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                         + actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]])
        # 3
        Rec_AcqExpMor_up['AccIntr'].loc[Rec_AcqExpMor_up.index[i]] = discount_rate * (Rec_AcqExpMor_up['Opening'].loc[Rec_AcqExpMor_up.index[i]]
                                                                                      + Rec_AcqExpMor_up['NewAcqExp'].loc[Rec_AcqExpMor_up.index[i]])
        # 4
        sum_coverageUni = 0
        for j in range(i, Total_years):
            sum_coverageUni = sum_coverageUni + \
                coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[j]]
        Rec_AcqExpMor_up['AmorExp'].loc[Rec_AcqExpMor_up.index[i]] = -(((Rec_AcqExpMor_up['Opening'].loc[Rec_AcqExpMor_up.index[i]]
                                                                         + Rec_AcqExpMor_up['NewAcqExp'].loc[Rec_AcqExpMor_up.index[i]]
                                                                         + Rec_AcqExpMor_up['AccIntr'].loc[Rec_AcqExpMor_up.index[i]]
                                                                         ) * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]])
                                                                       / sum_coverageUni)
        # 5
        Rec_AcqExpMor_up['Closes'].loc[Rec_AcqExpMor_up.index[i]] = (Rec_AcqExpMor_up['Opening'].loc[Rec_AcqExpMor_up.index[i]]
                                                                      + Rec_AcqExpMor_up['NewAcqExp'].loc[Rec_AcqExpMor_up.index[i]]
                                                                      + Rec_AcqExpMor_up['AccIntr'].loc[Rec_AcqExpMor_up.index[i]]
                                                                      + Rec_AcqExpMor_up['AmorExp'].loc[Rec_AcqExpMor_up.index[i]])
        temp = Rec_AcqExpMor_up['Closes'].loc[Rec_AcqExpMor_up.index[i]]
    
    Stat_Profloss_up = Stat_Profloss.copy()
    for i, row in Stat_Profloss_up.iterrows():
        # 1
        Stat_Profloss_up['Rel_CSM'].loc[Stat_Profloss_up.index[i]] = - Rec_CSM_updated['Changes_Rel'].loc[Rec_CSM_updated.index[i]]
        # 2
        Stat_Profloss_up['Rel_RA'].loc[Stat_Profloss_up.index[i]] = - Rec_RA_updated['Changes_Rel'].loc[Rec_RA_updated.index[i]]
        # 3
        Stat_Profloss_up['Exp_Claim'].loc[Stat_Profloss_up.index[i]] = - actual_cashflow_updated['Claims'].loc[actual_cashflow_updated.index[i]]
        # 4
        Stat_Profloss_up['Exp_Expen'].loc[Stat_Profloss_up.index[i]] = -(actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                         + actual_cashflow_updated['Maint_Exp_Attr'].loc[actual_cashflow_updated.index[i]])
        # 5
        Stat_Profloss_up['Rec_AcqCasFl'].loc[Stat_Profloss_up.index[i]] = -Rec_AcqExpMor_up['AmorExp'].loc[Rec_AcqExpMor_up.index[i]]
        # 6
        Stat_Profloss_up['Ins_SerRev'].loc[Stat_Profloss_up.index[i]] = (Stat_Profloss_up['Rel_CSM'].loc[Stat_Profloss_up.index[i]] +
                                                                         Stat_Profloss_up['Rel_RA'].loc[Stat_Profloss_up.index[i]] +
                                                                         Stat_Profloss_up['Exp_Claim'].loc[Stat_Profloss_up.index[i]] +
                                                                         Stat_Profloss_up['Exp_Expen'].loc[Stat_Profloss_up.index[i]] +
                                                                         Stat_Profloss_up['Rec_AcqCasFl'].loc[Stat_Profloss_up.index[i]])
        # 7
        Stat_Profloss_up['Claim_Incur'].loc[Stat_Profloss_up.index[i]] = actual_cashflow_updated['Claims'].loc[actual_cashflow_updated.index[i]]
        # 8
        Stat_Profloss_up['Exp_Incur'].loc[Stat_Profloss_up.index[i]] = (actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]] +
                                                                        actual_cashflow_updated['Maint_Exp_Attr'].loc[actual_cashflow_updated.index[i]])
        # 9
        Stat_Profloss_up['Amor_AcqCasFlo'].loc[Stat_Profloss_up.index[i]] = Rec_AcqExpMor_up['AmorExp'].loc[Rec_AcqExpMor_up.index[i]]
        # 10
        Stat_Profloss_up['Ins_SerExp'].loc[Stat_Profloss_up.index[i]] = (Stat_Profloss_up['Claim_Incur'].loc[Stat_Profloss_up.index[i]] +
                                                                         Stat_Profloss_up['Exp_Incur'].loc[Stat_Profloss_up.index[i]] +
                                                                         Stat_Profloss_up['Amor_AcqCasFlo'].loc[Stat_Profloss_up.index[i]])
        # 11
        Stat_Profloss_up['Other_Exp'].loc[Stat_Profloss_up.index[i]] = (actual_cashflow_updated['Acq_Exp_N_Attr'].loc[actual_cashflow_updated.index[i]] +
                                                                        actual_cashflow_updated['Maint_Exp_N_Attr'].loc[actual_cashflow_updated.index[i]])
        # 12
        Stat_Profloss_up['Inv_income'].loc[Stat_Profloss_up.index[i]] = (asset_ret_rate * (Rec_TotContLiab_up['Opening'].loc[Rec_TotContLiab_up.index[i]] +
                                                                                           Rec_TotContLiab_up['NewBusiness'].loc[Rec_TotContLiab_up.index[i]])
                                                                         + asset_ret_rate * (actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]] +
                                                                                             actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]] +
                                                                                             actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]] +
                                                                                             actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]]))
        # 13
        Stat_Profloss_up['Ins_FinExp'].loc[Stat_Profloss_up.index[i]] = - Rec_TotContLiab_up['FinExp'].loc[Rec_TotContLiab_up.index[i]]
        # 14
        Stat_Profloss_up['Fin_GainLoss'].loc[Stat_Profloss_up.index[i]] = (Stat_Profloss_up['Inv_income'].loc[Stat_Profloss_up.index[i]] +
                                                                           Stat_Profloss_up['Ins_FinExp'].loc[Stat_Profloss_up.index[i]])
        # 15
        Stat_Profloss_up['Prof_loss'].loc[Stat_Profloss_up.index[i]] = (Stat_Profloss_up['Ins_SerRev'].loc[Stat_Profloss_up.index[i]] +
                                                                        Stat_Profloss_up['Ins_SerExp'].loc[Stat_Profloss_up.index[i]] +
                                                                        Stat_Profloss_up['Other_Exp'].loc[Stat_Profloss_up.index[i]] +
                                                                        Stat_Profloss_up['Fin_GainLoss'].loc[Stat_Profloss_up.index[i]])
        
    # filler_ids = [str(uuid.uuid4()) for i in range(22)]
    filler_ids = [    "ace3a15f-7306-4bb3-ad25-ab4251d6dfff",    "4a55e941-dd42-4f9d-a867-0a0396d94a21",    "743c0ab8-4a8a-4fe9-aee0-d853deb1f43d",    "e8fc830b-3b98-49e1-b199-9c8030f75191",    "5fa9696f-7280-4ea3-b475-2db3c0b085c3",    "e280de91-9672-4446-841b-9d49c415fecc",    "9a887fc7-b823-4519-a599-dbb12bec543b",    "89ec0703-b707-448c-a3a2-dbef3fa530c4",    "fed7afc3-8527-44fd-b14b-8792277efd6f",    "8a53593d-cf43-49c7-adc1-31694df7b3f2",    "2bb2c509-a84b-4475-b766-45ef800d3e21",    "d831cc06-5a0c-46e9-ac60-9f9554d7bfce",    "bd994325-e135-4590-b5a2-f2e73b689aef",    "0df1a518-269f-4127-bb58-00c369cb8e09",    "5a823654-6a4f-4f2a-8393-021331abcbde",    "2072d0b9-2701-43ba-995a-788bcf043b44",    "3ad17a3c-f5ad-4dda-aa74-66800ef5ee10",    "849b31a5-3637-4450-ac21-ff88250a53e7",    "f1beffa0-96fc-43ab-80e2-1c1f2324a903",    "293c42db-926e-4455-a9a4-806a82ae8a84",    "885ca9fc-b4e4-41b0-b8b5-5fbebd5b75fc",    "3771711e-1c55-422f-9205-fd55c343b34d"]
    active_flags = [1] * 22    
    created_bys = ['User1','User2','User3','User4','User5','User6','User7','User8','User9','User10','User11','User12','User13','User14','User15','User16','User17','User18','User19','User20','User21','User22']
    
    created_dates = [datetime.now()] * 22
    modified_bys = ['User1','User2','User3','User4','User5','User6','User7','User8','User9','User10','User11','User12','User13','User14','User15','User16','User17','User18','User19','User20','User21','User22']
    modified_dates = [datetime.now()] * 22   
    
    actual_cashflow = actual_cashflow_updated.reset_index()
    actual_cashflow['Run_ID'] = filler_ids[:22]
    actual_cashflow['Actual_Cashflow_ID'] = filler_ids[:22]
    actual_cashflow['Active_Flag'] = active_flags[:22]
    actual_cashflow['Created_By'] = created_bys[:22]
    actual_cashflow['Created_Date'] = created_dates[:22]
    actual_cashflow['Modified_By'] = modified_bys[:22]
    actual_cashflow['Modified_Date'] = modified_dates[:22]
    
    run = pd.DataFrame()
    run['Run_ID'] = filler_ids[:22]
    run['Run_Name']=['run1','run2','run3','run4','run5','run6','run7','run8','run9','run10','run11','run12','run13','run14','run15','run16','run17','run18','run19','run20','run21','run22']
    run['Conf_ID'] = filler_ids[:22]
    run['Reporting_Date']=[datetime.now()] * 22
    run['Active_Flag'] = active_flags[:22]
    run['Created_By'] = created_bys[:22]
    run['Created_Date'] = created_dates[:22]
    run['Modified_By'] = modified_bys[:22]
    run['Modified_Date'] = modified_dates[:22]
    
    coverage_uni_recon = coverage_uni_recon_updated.reset_index()
    coverage_uni_recon['Coverage_Units_Rec_ID'] = filler_ids[:22]
    coverage_uni_recon['Run_ID'] = filler_ids[:22]
    coverage_uni_recon['Active_Flag'] = active_flags[:22]
    coverage_uni_recon['Created_By'] = created_bys[:22]
    coverage_uni_recon['Created_Date'] = created_dates[:22]
    coverage_uni_recon['Modified_By'] = modified_bys[:22]
    coverage_uni_recon['Modified_Date'] = modified_dates[:22] 
    
    
    Liab_init_reco = Liab_init_reco.reset_index()
    
    liability_init_rec = pd.DataFrame() 
    liability_init_rec['Liability_Init_Rec_ID'] = filler_ids[:1]
    liability_init_rec['Run_ID'] = filler_ids[:1]
    liability_init_rec['index'] = Liab_init_reco['index']
    liability_init_rec['PV_Prem'] = Liab_init_reco['PV_Prem']
    liability_init_rec['PV_RenComm'] = Liab_init_reco['PV_RenComm']
    liability_init_rec['PV_Claims'] = Liab_init_reco['PV_Claims']
    liability_init_rec['PV_DirAqis'] = Liab_init_reco['PV_DirAqis']
    liability_init_rec['PV_RiskAdj'] = Liab_init_reco['PV_RiskAdj']
    liability_init_rec['Total'] = Liab_init_reco['Total']
    liability_init_rec['CSM_Init'] = Liab_init_reco['CSM_Init']
    liability_init_rec['Liab_Init'] = Liab_init_reco['Liab_Init']
    liability_init_rec['Active_Flag'] = active_flags[:1]
    liability_init_rec['Created_By'] = created_bys[:1]
    liability_init_rec['Created_Date'] = created_dates[:1]
    liability_init_rec['Modified_By'] = modified_bys[:1]
    liability_init_rec['Modified_Date'] = modified_dates[:1]
    
    Rec_BEL_updated = Rec_BEL_updated.reset_index()
    
    rec_bel = pd.DataFrame()
    rec_bel['Rec_BEL_ID'] = filler_ids[:22]
    rec_bel['Run_ID'] = filler_ids[:22]
    rec_bel['index'] = Rec_BEL_updated['index']
    rec_bel['Opening'] = Rec_BEL_updated['Opening']
    rec_bel['NewBusiness'] = Rec_BEL_updated['NewBusiness']
    rec_bel['Assump'] = Rec_BEL_updated['Assump']
    rec_bel['ExpInflow'] = Rec_BEL_updated['ExpInflow']
    rec_bel['ExpOutFlow'] = Rec_BEL_updated['ExpOutFlow']
    rec_bel['FinExp'] = Rec_BEL_updated['FinExp']
    rec_bel['Changes_Exp'] = Rec_BEL_updated['Changes_Exp']
    rec_bel['Changes_Rel'] = Rec_BEL_updated['Changes_Rel']
    rec_bel['Closes'] = Rec_BEL_updated['Closes']
    rec_bel['Active_Flag'] = active_flags[:22]
    rec_bel['Created_By'] = created_bys[:22]
    rec_bel['Created_Date'] = created_dates[:22]
    rec_bel['Modified_By'] = modified_bys[:22]
    rec_bel['Modified_Date'] = modified_dates[:22]
    
    
    Rec_RA_updated = Rec_RA_updated.reset_index()
    
    rec_ra = pd.DataFrame()
    rec_ra['Rec_RA_ID'] = filler_ids[:22]
    rec_ra['Run_ID'] = filler_ids[:22]
    rec_ra['index'] = Rec_RA_updated['index']
    rec_ra['Opening'] = Rec_RA_updated['Opening']
    rec_ra['NewBusiness'] = Rec_RA_updated['NewBusiness']
    rec_ra['Assump'] = Rec_RA_updated['Assump']
    rec_ra['ExpInflow'] = Rec_RA_updated['ExpInflow']
    rec_ra['ExpOutFlow'] = Rec_RA_updated['ExpOutFlow']
    rec_ra['FinExp'] = Rec_RA_updated['FinExp']
    rec_ra['Changes_Exp'] = Rec_RA_updated['Changes_Exp']
    rec_ra['Changes_Rel'] = Rec_RA_updated['Changes_Rel']
    rec_ra['Closes'] = Rec_RA_updated['Closes']
    rec_ra['Active_Flag'] = active_flags[:22]
    rec_ra['Created_By'] = created_bys[:22]
    rec_ra['Created_Date'] = created_dates[:22]
    rec_ra['Modified_By'] = modified_bys[:22]
    rec_ra['Modified_Date'] = modified_dates[:22]
    
    Rec_CSM_updated = Rec_CSM_updated.reset_index()
    
    rec_csm = pd.DataFrame()
    rec_csm['Rec_CSM_ID'] = filler_ids[:22]
    rec_csm['Run_ID'] = filler_ids[:22]
    rec_csm['index'] = Rec_CSM_updated['index']
    rec_csm['Opening'] = Rec_CSM_updated['Opening']
    rec_csm['NewBusiness'] = Rec_CSM_updated['NewBusiness']
    rec_csm['Assump'] = Rec_CSM_updated['Assump']
    rec_csm['ExpInflow'] = Rec_CSM_updated['ExpInflow']
    rec_csm['ExpOutFlow'] = Rec_CSM_updated['ExpOutFlow']
    rec_csm['FinExp'] = Rec_CSM_updated['FinExp']
    rec_csm['Changes_Exp'] = Rec_CSM_updated['Changes_Exp']
    rec_csm['Changes_Rel'] = Rec_CSM_updated['Changes_Rel']
    rec_csm['Closes'] = Rec_CSM_updated['Closes']
    rec_csm['Loss_Comp'] = [0] * 22
    rec_csm['Active_Flag'] = active_flags[:22]
    rec_csm['Created_By'] = created_bys[:22]
    rec_csm['Created_Date'] = created_dates[:22]
    rec_csm['Modified_By'] = modified_bys[:22]
    rec_csm['Modified_Date'] = modified_dates[:22]
    
    Rec_TotContLiab_up = Rec_TotContLiab_up.reset_index()
    
    rec_totcontliab = pd.DataFrame()
    rec_totcontliab['Rec_TotContLiab_ID'] = filler_ids[:22]
    rec_totcontliab['Run_ID'] = filler_ids[:22]
    rec_totcontliab['index'] = Rec_TotContLiab_up['index']
    rec_totcontliab['Opening'] = Rec_TotContLiab_up['Opening']
    rec_totcontliab['NewBusiness'] = Rec_TotContLiab_up['NewBusiness']
    rec_totcontliab['Assump'] = Rec_TotContLiab_up['Assump']
    rec_totcontliab['ExpInflow'] = Rec_TotContLiab_up['ExpInflow']
    rec_totcontliab['ExpOutFlow'] = Rec_TotContLiab_up['ExpOutFlow']
    rec_totcontliab['FinExp'] = Rec_TotContLiab_up['FinExp']
    rec_totcontliab['Changes_Exp'] = Rec_TotContLiab_up['Changes_Exp']
    rec_totcontliab['Changes_Rel'] = Rec_TotContLiab_up['Changes_Rel']
    rec_totcontliab['Closes'] = Rec_TotContLiab_up['Closes']
    rec_totcontliab['Active_Flag'] = active_flags[:22]
    rec_totcontliab['Created_By'] = created_bys[:22]
    rec_totcontliab['Created_Date'] = created_dates[:22]
    rec_totcontliab['Modified_By'] = modified_bys[:22]
    rec_totcontliab['Modified_Date'] = modified_dates[:22]
    
    Rec_AcqExpMor_up = Rec_AcqExpMor_up.reset_index()
    
    rec_acqexpmor_up = pd.DataFrame()
    rec_acqexpmor_up['Rec_AcqExpMor_ID'] = filler_ids[:22]
    rec_acqexpmor_up['Run_ID'] = filler_ids[:22]
    rec_acqexpmor_up['index'] = Rec_AcqExpMor_up['index']
    rec_acqexpmor_up['Opening'] = Rec_AcqExpMor_up['Opening']
    rec_acqexpmor_up['NewAcqExp'] = Rec_AcqExpMor_up['NewAcqExp']
    rec_acqexpmor_up['AccIntr'] = Rec_AcqExpMor_up['AccIntr']
    rec_acqexpmor_up['AmorExp'] = Rec_AcqExpMor_up['AmorExp']
    rec_acqexpmor_up['Closes'] = Rec_AcqExpMor_up['Closes']
    rec_acqexpmor_up['Active_Flag'] = active_flags[:22]
    rec_acqexpmor_up['Created_By'] = created_bys[:22]
    rec_acqexpmor_up['Created_Date'] = created_dates[:22]
    rec_acqexpmor_up['Modified_By'] = modified_bys[:22]
    rec_acqexpmor_up['Modified_Date'] = modified_dates[:22]
    
    stat_profloss_up = Stat_Profloss_up
    stat_profloss_up['Stat_Profloss_ID'] = filler_ids[:22]
    stat_profloss_up['Run_ID'] = filler_ids[:22]
    stat_profloss_up['index'] = [i for i in range(22)]
    stat_profloss_up['Rel_CSM'] = Stat_Profloss_up['Rel_CSM']
    stat_profloss_up['Rel_RA'] = Stat_Profloss_up['Rel_RA']
    stat_profloss_up['Exp_Claim'] = Stat_Profloss_up['Exp_Claim']
    stat_profloss_up['Exp_Expen'] = Stat_Profloss_up['Exp_Expen']
    stat_profloss_up['Rec_AcqCasFl'] = Stat_Profloss_up['Rec_AcqCasFl']
    stat_profloss_up['Ins_SerRev'] = Stat_Profloss_up['Ins_SerRev']
    stat_profloss_up['Claim_Incur'] = Stat_Profloss_up['Claim_Incur']
    stat_profloss_up['Exp_Incur'] = Stat_Profloss_up['Exp_Incur']
    stat_profloss_up['Amor_AcqCasFlo'] = Stat_Profloss_up['Amor_AcqCasFlo']
    stat_profloss_up['Ins_SerExp'] = Stat_Profloss_up['Ins_SerExp']
    stat_profloss_up['Other_Exp'] = Stat_Profloss_up['Other_Exp']
    stat_profloss_up['Inv_income'] = Stat_Profloss_up['Inv_income']
    stat_profloss_up['Ins_FinExp'] = Stat_Profloss_up['Ins_FinExp']
    stat_profloss_up['Fin_GainLoss'] = Stat_Profloss_up['Fin_GainLoss']
    stat_profloss_up['Prof_loss'] = Stat_Profloss_up['Prof_loss']
    stat_profloss_up['Active_Flag'] = active_flags[:22]
    stat_profloss_up['Created_By'] = created_bys[:22]
    stat_profloss_up['Created_Date'] = created_dates[:22]
    stat_profloss_up['Modified_By'] = modified_bys[:22]
    stat_profloss_up['Modified_Date'] = modified_dates[:22]
    
    
    
    run_input = df_temp
    run_input['Run_ID'] = filler_ids[:22]
    run_input['Run_Input_ID'] = filler_ids[:22]
    run_input['Active_Flag'] = active_flags[:22]
    run_input['Created_By'] = created_bys[:22]
    run_input['Created_Date'] = created_dates[:22]
    run_input['Modified_By'] = modified_bys[:22]
    run_input['Modified_Date'] = modified_dates[:22]
    
    return run,run_input,coverage_uni_recon, actual_cashflow, liability_init_rec, rec_bel, rec_ra, rec_csm, rec_totcontliab, rec_acqexpmor_up , stat_profloss_up



def enter_details_to_db():
    input_data = pd.read_csv(r"New_Data_Random_inputs.csv",index_col=0)
    input_data1=input_data.loc[0]
    
    run , run_input,coverage_uni_recon, actual_cashflow, Liab_init_reco, Rec_BEL_updated, Rec_RA_updated, Rec_CSM_updated, Rec_TotContLiab_up, Rec_AcqExpMor_up, Stat_Profloss_up = calculate(input_data1)
    
    coverage_uni_recon.to_csv('coverage_uni_recon.csv')
    actual_cashflow.to_csv('actual_cashflow.csv')
    Liab_init_reco.to_csv('Liab_init_reco.csv')
    Rec_BEL_updated.to_csv('Rec_BEL_updated.csv')
    Rec_RA_updated.to_csv('Rec_RA_updated.csv')
    Rec_CSM_updated.to_csv('Rec_CSM_updated.csv')
    Rec_TotContLiab_up.to_csv('Rec_TotContLiab_up.csv')
    Rec_AcqExpMor_up.to_csv('Rec_AcqExpMor_up.csv')
    Stat_Profloss_up.to_csv('Stat_Profloss_up.csv')
    
    
    
    send_bulk_records(run,'http://127.0.0.1:8000/api/insert/Run')
    # send_bulk_records(coverage_uni_recon,'http://127.0.0.1:8000/api/coverage-units-rec')
    # send_bulk_records(actual_cashflow,'http://127.0.0.1:8000/api/actual-cashflow')
    # send_bulk_records(Liab_init_reco,'http://127.0.0.1:8000/api/liability-init-rec')
    # send_bulk_records(Rec_BEL_updated,'http://127.0.0.1:8000/api/rec-bel-updated')
    

enter_details_to_db()
# input_data = pd.read_csv(r"New_Data_Random_inputs.csv",index_col=0)
# input_data1=input_data.loc[0]

# output_data=calculate(input_data1)