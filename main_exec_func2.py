"""
Created on Wed Aug 28 02:04:10 2024

@author: AMiGO
"""

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
from datetime import datetime
from numpy_financial import npv, irr


def calculate(df_temp):
    df_temp=df_temp.loc[:,"sum_assured":]
    
    sum_assured = df_temp['sum_assured'][0].astype(float).item()
    Num_Policies = df_temp['Num_Policies'][0].astype(float).item()
    prem_rate_per1000 = df_temp['prem_rate_per1000']
    policy_fees = df_temp['policy_fees'][0].astype(int).item()
    policy_Init_comm = df_temp['policy_Init_comm'][0].astype(float).item()
    policy_yearly_comm = df_temp['policy_yearly_comm'][0].astype(float).item()
    acq_direct_expenses = df_temp['acq_direct_expenses'][0].astype(float).item()
    acq_indirect_expense = df_temp['acq_indirect_expense'][0].astype(float).item()
    main_direct_expenses = df_temp['main_direct_expenses'][0].astype(float).item()
    main_indirect_expenses = df_temp['main_indirect_expenses'][0].astype(float).item()
    Total_years = df_temp['Total_years'][0].astype(int).item()
    discount_rate = df_temp['discount_rate'][0].astype(float).item()
    asset_ret_rate = df_temp['asset_ret_rate'][0].astype(float).item()
    CSM_ret_Rate = df_temp['CSM_ret_Rate'][0].astype(float).item()
    risk_adjst_rate = df_temp['risk_adjst_rate'][0].astype(float).item()
    mortality = df_temp['mortality']
    lapse = df_temp['lapse']
    
    # Create a DataFrame from the user input
    Opening = [0]*Total_years
    Deaths = [0]*Total_years
    Lapses = [0]*Total_years
    Closes = [0]*Total_years
    
    coverage_uni_recon = pd.DataFrame(
        {'Opening': Opening,
         'Deaths': Deaths,
         'Lapses': Lapses,
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
        print(i)
        coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]] = temp
        
        coverage_uni_recon_updated['Deaths'].loc[coverage_uni_recon_updated.index[i]] = mortality[i] * \
            coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
        
        coverage_uni_recon_updated['Lapses'].loc[coverage_uni_recon_updated.index[i]] = lapse[i] * \
            coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
        
        coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] = (coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
                                                                                          - coverage_uni_recon_updated['Deaths'].loc[coverage_uni_recon_updated.index[i]]
                                                                                          - coverage_uni_recon_updated['Lapses'].loc[coverage_uni_recon_updated.index[i]])
        if i < len(coverage_uni_recon):
            temp = coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]]
        
        if (coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] < 0):
            coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] = 0
    
    coverage_uni_recon_updated_T = coverage_uni_recon_updated.T
    print(coverage_uni_recon_updated.T)
    
    # cash flow calculations
    actual_cashflow_updated = actual_cashflow.copy()
    
    for i, row in actual_cashflow_updated.iterrows():
        
        # 1
        actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]] = (
            sum_assured * prem_rate_per1000[i]/1000 + policy_fees) * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
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
    
    
    # Risk adjustment calculations
    
    risk_adjust = actual_cashflow_updated['Claims'] * risk_adjst_rate
    print(risk_adjust.T)
    
    # Liability on Initial Recognition
    
    PV_Prem = npv(discount_rate, actual_cashflow_updated['Premiums'])
    PV_RenComm = npv(discount_rate, actual_cashflow_updated['Renewal_Comm'])
    # PV_DirExpen = npv ( discount_rate, actual_cashflow_updated['Maint_Exp_Attr'])
    PV_DirExpen = (actual_cashflow_updated['Maint_Exp_Attr'] / (1 + discount_rate) ** np.arange(
        1, len(actual_cashflow_updated['Maint_Exp_Attr']) + 1)).sum(axis=0)
    # PV_Claims  = npv ( discount_rate, actual_cashflow_updated['Claims'])
    PV_Claims = (actual_cashflow_updated['Claims'] / (1 + discount_rate) ** np.arange(
        1, len(actual_cashflow_updated['Claims']) + 1)).sum(axis=0)
    PV_DirAqis = (actual_cashflow_updated['Acquisition_Comm'].values +
                  actual_cashflow_updated['Acq_Exp_Attr'].values)[0]
    # PV_RiskAdj  = npv ( discount_rate, risk_adjust)
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
    
    # Reconciliation of Best Estimate Liabilities (BEL)
    
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

    # Reconciliation of Risk Adjustment (RA)
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
    
    # Reconciliation of Contractual Service Margin (CSM)
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
        # 9
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

    # Reconciliation of Total Contract Liability
    
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

    # Reconciliation of Acquisition Expense Amortization
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
    

    # Statement of Profit or Loss
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

    return {
        "Coverage Units Reconciliation": coverage_uni_recon_updated.reset_index(), 
        "Actual Risk Adjustment CFs" : actual_cashflow_updated.reset_index(),
        "Liability on Initial Recognition" : Liab_init_reco.reset_index(),
        "Reconciliation of Best Estimate Liabilities" : Rec_BEL_updated.reset_index(),
        "Reconciliation of Risk Adjustment" : Rec_RA_updated.reset_index(),
        "Reconciliation of Contractual Service Margin" : Rec_CSM_updated.reset_index(), 
        "Reconciliation of Total Contract Liability" : Rec_TotContLiab_up.reset_index(),
        "Reconciliation of Acquisition Expense Amortization" : Rec_AcqExpMor_up.reset_index(), 
        "Statement of Profit or Loss" : Stat_Profloss_up.reset_index()
        }


def IFRS4_calculate(df_temp):
    df_temp=df_temp.loc[:,"sum_assured":]

    sum_assured = df_temp['sum_assured'][0].astype(float).item()
    Num_Policies = df_temp['Num_Policies'][0].astype(float).item()
    prem_rate_per1000 = df_temp['prem_rate_per1000']
    policy_fees = df_temp['policy_fees'][0].astype(int).item()
    policy_Init_comm = df_temp['policy_Init_comm'][0].astype(float).item()
    policy_yearly_comm = df_temp['policy_yearly_comm'][0].astype(float).item()
    acq_direct_expenses = df_temp['acq_direct_expenses'][0].astype(float).item()
    acq_indirect_expense = df_temp['acq_indirect_expense'][0].astype(float).item()
    main_direct_expenses = df_temp['main_direct_expenses'][0].astype(float).item()
    main_indirect_expenses = df_temp['main_indirect_expenses'][0].astype(float).item()
    Total_years = df_temp['Total_years'][0].astype(int).item()
    discount_rate = df_temp['discount_rate'][0].astype(float).item()
    asset_ret_rate = df_temp['asset_ret_rate'][0].astype(float).item()
    CSM_ret_Rate = df_temp['CSM_ret_Rate'][0].astype(float).item()
    risk_adjst_rate = df_temp['risk_adjst_rate'][0].astype(float).item()
    mortality = df_temp['mortality']
    lapse = df_temp['lapse']

    # Create a DataFrame from the user input
    Opening = [0]*Total_years
    Deaths = [0]*Total_years
    Lapses = [0]*Total_years
    Closes = [0]*Total_years

    coverage_uni_recon = pd.DataFrame(
        {'Opening': Opening,
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

    Rec_BEL = pd.DataFrame(
        {'Opening': Opening,
         'ExpInflow': ExpInflow,
         'ExpOutFlow': ExpOutFlow,
         'FinExp': FinExp,
         'Changes_Exp': Changes_Exp,
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

    Gross_Prem = [0]*Total_years
    Invest_inc = [0]*Total_years
    Total_Income = [0]*Total_years
    Claim_Incur = [0]*Total_years
    Exp_Incur = [0]*Total_years
    Amor_AcqCasFlo = [0]*Total_years
    Chnge_Insu_Liab = [0]*Total_years
    Total_Expenses = [0]*Total_years
    Prof_loss = [0]*Total_years

    Stat_Profloss = pd.DataFrame(
        {'Gross_Prem': Gross_Prem,
         'Invest_inc': Invest_inc,
         'Total_Income': Total_Income,

         'Claim_Incur': Claim_Incur,
         'Exp_Incur': Exp_Incur,
         'Amor_AcqCasFlo': Amor_AcqCasFlo,
         'Chnge_Insu_Liab': Chnge_Insu_Liab,
         'Total_Expenses': Total_Expenses,

         'Prof_loss': Prof_loss,

         })

    # Coverage units reconcilation
    coverage_uni_recon_updated = coverage_uni_recon.copy()
    temp = Num_Policies

    for i, row in coverage_uni_recon_updated.iterrows():

        coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]] = temp

        coverage_uni_recon_updated['Deaths'].loc[coverage_uni_recon_updated.index[i]] = mortality[i] * \
            coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]

        coverage_uni_recon_updated['Lapses'].loc[coverage_uni_recon_updated.index[i]] = lapse[i] * \
            coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]

        coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] = (coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
                                                                                          - coverage_uni_recon_updated['Deaths'].loc[coverage_uni_recon_updated.index[i]]
                                                                                          - coverage_uni_recon_updated['Lapses'].loc[coverage_uni_recon_updated.index[i]])

        if i < len(coverage_uni_recon):
            temp = coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]]

        if (coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] < 0):
            coverage_uni_recon_updated['Closes'].loc[coverage_uni_recon_updated.index[i]] = 0

    IFRS4_coverage_uni_recon_updated_T = coverage_uni_recon_updated.T
    print(IFRS4_coverage_uni_recon_updated_T)

    # cash flow calculations

    actual_cashflow_updated = actual_cashflow.copy()

    for i, row in actual_cashflow_updated.iterrows():

        # 1
        actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]] = (
            sum_assured * prem_rate_per1000[i]/1000 + policy_fees) * coverage_uni_recon_updated['Opening'].loc[coverage_uni_recon_updated.index[i]]
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


    # Risk adjustment calculations

    risk_adjust = actual_cashflow_updated['Claims'] * risk_adjst_rate
    print(risk_adjust.T)

    # Liability on Initial Recognition

    PV_Prem = actual_cashflow_updated['Premiums'].sum()
    PV_RenComm = actual_cashflow_updated['Renewal_Comm'].sum()
    PV_DirExpen = actual_cashflow_updated['Maint_Exp_Attr'].sum()
    PV_Claims = actual_cashflow_updated['Claims'].sum()
    PV_DirAqis = (actual_cashflow_updated['Acquisition_Comm'].values +
                  actual_cashflow_updated['Acq_Exp_Attr'].values)[0]
    Total = PV_Prem + PV_RenComm + PV_DirExpen + PV_Claims + PV_DirAqis
    
    Liab_Init = -Total
    Liab_init_reco = pd.DataFrame({
        'PV_Prem': PV_Prem,
        'PV_RenComm': PV_RenComm,
        'PV_DirExpen': PV_DirExpen,
        'PV_Claims': PV_Claims,
        'PV_DirAqis': PV_DirAqis,
        'Total': Total,
        'Liab_Init': Liab_Init}, index=[0])


    # Reconciliation of Best Estimate Liabilities (BEL)

    Rec_BEL_updated = Rec_BEL.copy()
    temp = Liab_Init
    for i, row in Rec_BEL_updated.iterrows():
        # 1
        Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]] = temp
        # 2
        Rec_BEL_updated['ExpInflow'].loc[Rec_BEL_updated.index[i]] = actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]]
        # 3
        Rec_BEL_updated['ExpOutFlow'].loc[Rec_BEL_updated.index[i]] = (actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Maint_Exp_Attr'].loc[actual_cashflow_updated.index[i]]
                                                                       + actual_cashflow_updated['Claims'].loc[actual_cashflow_updated.index[i]]
                                                                       )
        # 4
        Rec_BEL_updated['FinExp'].loc[Rec_BEL_updated.index[i]] = discount_rate * (Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]]) + discount_rate * (actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]] +
                                                                                                                                                                + actual_cashflow_updated['Acquisition_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                                                                                                                + actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]]
                                                                                                                                                                + actual_cashflow_updated['Acq_Exp_Attr'].loc[actual_cashflow_updated.index[i]])
        # 5
        Rec_BEL_updated['Changes_Exp'].loc[Rec_BEL_updated.index[i]] = 0

        # 6
        Rec_BEL_updated['Closes'].loc[Rec_BEL_updated.index[i]] = (Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['ExpInflow'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['ExpOutFlow'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['FinExp'].loc[Rec_BEL_updated.index[i]] +
                                                                    Rec_BEL_updated['Changes_Exp'].loc[Rec_BEL_updated.index[i]])
        temp = Rec_BEL_updated['Closes'].loc[Rec_BEL_updated.index[i]]

    # IFRS4_Rec_BEL_updated_T = Rec_BEL_updated.T
    # IFRS4_Rec_BEL_updated_T.index = ['Opening', 'Expected Cash Inflows', 'Expected Cash Outflows', 'Insurance Finance Expense',
    #                                  'Changes Related to Current Services: Experience', 'Closing']
    # print(IFRS4_Rec_BEL_updated_T)

    # Reconciliation of Acquisition Expense Amortization
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
        Stat_Profloss_up['Gross_Prem'].loc[Stat_Profloss_up.index[i]] = actual_cashflow_updated['Premiums'].loc[actual_cashflow_updated.index[i]]
        # 2
        Stat_Profloss_up['Invest_inc'].loc[Stat_Profloss_up.index[i]] = asset_ret_rate * Stat_Profloss_up['Gross_Prem'].loc[Stat_Profloss_up.index[i]]
        # 3
        Stat_Profloss_up['Total_Income'].loc[Stat_Profloss_up.index[i]] = (Stat_Profloss_up['Gross_Prem'].loc[Stat_Profloss_up.index[i]] +
                                                                           Stat_Profloss_up['Invest_inc'].loc[Stat_Profloss_up.index[i]])
        # 4
        Stat_Profloss_up['Claim_Incur'].loc[Stat_Profloss_up.index[i]] = actual_cashflow_updated['Claims'].loc[actual_cashflow_updated.index[i]]
        # 5
        Stat_Profloss_up['Exp_Incur'].loc[Stat_Profloss_up.index[i]] = (actual_cashflow_updated['Renewal_Comm'].loc[actual_cashflow_updated.index[i]] +
                                                                        actual_cashflow_updated['Maint_Exp_Attr'].loc[actual_cashflow_updated.index[i]])
        # 6
        Stat_Profloss_up['Amor_AcqCasFlo'].loc[Stat_Profloss_up.index[i]] = Rec_AcqExpMor_up['AmorExp'].loc[Rec_AcqExpMor_up.index[i]]
        # 7
        Stat_Profloss_up['Chnge_Insu_Liab'].loc[Stat_Profloss_up.index[i]] = (Rec_BEL_updated['Opening'].loc[Rec_BEL_updated.index[i]] -
                                                     Rec_BEL_updated['Closes'].loc[Rec_BEL_updated.index[i]])
        # 8
        Stat_Profloss_up['Total_Expenses'].loc[Stat_Profloss_up.index[i]] = (Stat_Profloss_up['Claim_Incur'].loc[Stat_Profloss_up.index[i]] +
                                                                             Stat_Profloss_up['Exp_Incur'].loc[Stat_Profloss_up.index[i]] +
                                                                             Stat_Profloss_up['Amor_AcqCasFlo'].loc[Stat_Profloss_up.index[i]] +
                                                                             Stat_Profloss_up['Chnge_Insu_Liab'].loc[Stat_Profloss_up.index[i]])
        # 9
        Stat_Profloss_up['Prof_loss'].loc[Stat_Profloss_up.index[i]] = (Stat_Profloss_up['Total_Income'].loc[Stat_Profloss_up.index[i]] +
                                                                        Stat_Profloss_up['Total_Expenses'].loc[Stat_Profloss_up.index[i]])

    return {"Coverage Units Reconciliation": coverage_uni_recon_updated.reset_index(),
            "Actual Cashflows": actual_cashflow_updated.reset_index(),
            "Liability on Initial Recognition":Liab_init_reco.reset_index(), 
            "Reconciliation of Best Estimate Liabilities":Rec_BEL_updated.reset_index(), 
            "Reconciliation of Acquisition Expense Amortization":Rec_AcqExpMor_up.reset_index(), 
            "Statement of Profit or Loss":Stat_Profloss_up.reset_index()}



def GMMvsIFRS4(output_data_GMM,output_data_ifrs):
    ifrs4_tbl=['Coverage Units Reconciliation', 'Actual Cashflows','Liability on Initial Recognition', 'Reconciliation of Acquisition Expense Amortization',
               'Reconciliation of Best Estimate Liabilities', 'Statement of Profit or Loss']
    
    GMM_tbl=['Coverage Units Reconciliation', 'Actual Risk Adjustment CFs', 'Liability on Initial Recognition', 'Reconciliation of Acquisition Expense Amortization',
               'Reconciliation of Best Estimate Liabilities', 'Statement of Profit or Loss']
    
    comp={}
    
    for t1, t2 in zip(ifrs4_tbl, GMM_tbl):
        comp_cols=list(set(output_data_GMM[t2].columns).intersection(set(output_data_ifrs[t1].columns)))
        comp[t1]={
            "GMM_tbl":list(output_data_GMM[t2].columns),
            "ifrs4_tbl":list(output_data_ifrs[t1].columns),
            "comp_cols":comp_cols, 
            "comp":((output_data_GMM[t2]-output_data_ifrs[t1])[comp_cols]).to_dict(orient='records')
        }
    return comp


input_data = pd.read_csv(r"main_input_v2.csv")
input_data1 = input_data[input_data["input_id"]==0]

output_data_GMM = calculate(input_data1)
output_data_ifrs = IFRS4_calculate(input_data1)

outputdata_comp = GMMvsIFRS4(output_data_GMM,output_data_ifrs)