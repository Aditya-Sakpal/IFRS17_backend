from datetime import datetime
import uuid
import time

import pandas as pd
import numpy as np
from numpy_financial import npv

from db_related.records_insertion import send_bulk_records

def calculate(df_temp,run_name):

    sum_assured = df_temp['sum_assured'].astype(float)
    Num_Policies = df_temp['Num_Policies'].astype(float)
    policy_fees = df_temp['policy_fees'].astype(int)
    prem_rate_per1000 = df_temp['prem_rate_per1000'].astype(float)
    policy_Init_comm = df_temp['policy_Init_comm'].astype(float)
    policy_yearly_comm = df_temp['policy_yearly_comm'].astype(float)
    acq_direct_expenses = df_temp['acq_direct_expenses'].astype(float)
    acq_indirect_expense = df_temp['acq_indirect_expense'].astype(float)
    main_direct_expenses = df_temp['main_direct_expenses'].astype(float)
    main_indirect_expenses = df_temp['main_indirect_expenses'].astype(float)
    Total_years = df_temp['Total_years'].astype(int)
    discount_rate = df_temp['discount_rate'].astype(float)
    asset_ret_rate = df_temp['asset_ret_rate'].astype(float)
    CSM_ret_Rate = df_temp['CSM_ret_Rate'].astype(float)
    risk_adjst_rate = df_temp['risk_adjst_rate'].astype(float)
    mortality = df_temp['mortality'].astype(float)
    lapse = df_temp['lapse'].astype(int)
    
    Opening = [0]*Total_years
    Deaths = [0]*Total_years
    Closes = [0]*Total_years
    Premiums = [0]*Total_years
    Acquisition_Comm = [0]*Total_years
    Renewal_Comm = [0]*Total_years
    Acq_Exp_Attr = [0]*Total_years
    Maint_Exp_Attr = [0]*Total_years
    Acq_Exp_N_Attr = [0]*Total_years
    Maint_Exp_N_Attr = [0]*Total_years
    Claims = [0]*Total_years
    Total_Net_CFs = [0]*Total_years
    NewBusiness = [0]*Total_years
    Assump = [0]*Total_years
    ExpInflow = [0]*Total_years
    ExpOutFlow = [0]*Total_years
    FinExp = [0]*Total_years
    Changes_Exp = [0]*Total_years
    Changes_Rel = [0]*Total_years
    Loss_Comp = [0]*Total_years
    NewAcqExp = [0]*Total_years
    AccIntr = [0]*Total_years
    AmorExp = [0]*Total_years
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
    
    filler_ids = []
    while len(filler_ids) < len(Opening):
        filler_ids.append(str(uuid.uuid4()))
        
    active_flags = [True] * len(Opening)
    created_bys = ["Infogis_User"] * len(Opening)
    modified_bys = ["Infogis_User"] * len(Opening)   
    created_dates = [datetime.now()] * len(Opening)
    modified_dates = [datetime.now()] * len(Opening)   

    # Create the initial DataFrame
    coverage_uni_recon = pd.DataFrame({
        'Opening': Opening,
        'Deaths': Deaths,
        'Lapses': lapse,
        'Closes': Closes
    })

    # Copy the DataFrame for updates
    coverage_uni_recon_updated = coverage_uni_recon.copy()

    # Pre-compute vectorized calculations
    openings = np.zeros(len(coverage_uni_recon))
    deaths = np.zeros(len(coverage_uni_recon))
    lapses = np.zeros(len(coverage_uni_recon))
    closes = np.zeros(len(coverage_uni_recon))

    # Initialize the temporary variable
    temp = Num_Policies

    # Perform vectorized calculations
    openings[:] = temp
    deaths = mortality * openings / 100
    lapses = lapse * openings / 100
    closes = openings - deaths - lapses

    # Ensure 'Closes' does not go below zero
    closes[closes < 0] = 0

    # Shift 'Closes' to update 'Opening' for the next period
    openings[1:] = closes[:-1]

    # Update the DataFrame
    coverage_uni_recon_updated['Opening'] = openings
    coverage_uni_recon_updated['Deaths'] = deaths
    coverage_uni_recon_updated['Lapses'] = lapses
    coverage_uni_recon_updated['Closes'] = closes

    # Reset index and add additional columns
    coverage_uni_recon_updated = coverage_uni_recon_updated.reset_index()
    coverage_uni_recon_updated['Coverage_Units_Rec_ID'] = filler_ids[:len(coverage_uni_recon_updated)]
    coverage_uni_recon_updated['Run_ID'] = filler_ids[:len(coverage_uni_recon_updated)]
    coverage_uni_recon_updated['Active_Flag'] = active_flags[:len(coverage_uni_recon_updated)]
    coverage_uni_recon_updated['Created_By'] = created_bys[:len(coverage_uni_recon_updated)]
    coverage_uni_recon_updated['Created_Date'] = created_dates[:len(coverage_uni_recon_updated)]
    coverage_uni_recon_updated['Modified_By'] = modified_bys[:len(coverage_uni_recon_updated)]
    coverage_uni_recon_updated['Modified_Date'] = modified_dates[:len(coverage_uni_recon_updated)]

    # Final result
    coverage_uni_recon = coverage_uni_recon_updated

                
    # Create the initial DataFrame
    actual_cashflow = pd.DataFrame({
        'Premiums': Premiums,
        'Acquisition_Comm': Acquisition_Comm,
        'Renewal_Comm': Renewal_Comm,
        'Acq_Exp_Attr': Acq_Exp_Attr,
        'Maint_Exp_Attr': Maint_Exp_Attr,
        'Acq_Exp_N_Attr': Acq_Exp_N_Attr,
        'Maint_Exp_N_Attr': Maint_Exp_N_Attr,
        'Claims': Claims,
        'Total_Net_CFs': Total_Net_CFs
    })

    # Create updated DataFrame
    actual_cashflow_updated = actual_cashflow.copy()

    # Pre-compute values for vectorized calculations
    openings = coverage_uni_recon_updated['Opening']
    deaths = coverage_uni_recon_updated['Deaths']
    premiums = (sum_assured * prem_rate_per1000 / 1000 + policy_fees) * openings

    # Vectorized calculations
    actual_cashflow_updated['Premiums'] = premiums
    actual_cashflow_updated['Acquisition_Comm'] = np.where(
        actual_cashflow_updated.index == 0, -policy_Init_comm * premiums, 0
    )
    actual_cashflow_updated['Renewal_Comm'] = np.where(
        actual_cashflow_updated.index == 0, 0, -policy_yearly_comm * premiums
    )
    actual_cashflow_updated['Acq_Exp_Attr'] = np.where(
        actual_cashflow_updated.index == 0, -acq_direct_expenses * openings, 0
    )
    actual_cashflow_updated['Maint_Exp_Attr'] = -main_direct_expenses * openings
    actual_cashflow_updated['Acq_Exp_N_Attr'] = np.where(
        actual_cashflow_updated.index == 0, -acq_indirect_expense * openings, 0
    )
    actual_cashflow_updated['Maint_Exp_N_Attr'] = -main_indirect_expenses * openings
    actual_cashflow_updated['Claims'] = -sum_assured * deaths

    # Calculate Total Net Cash Flows
    actual_cashflow_updated['Total_Net_CFs'] = (
        actual_cashflow_updated['Premiums']
        + actual_cashflow_updated['Acquisition_Comm']
        + actual_cashflow_updated['Renewal_Comm']
        + actual_cashflow_updated['Acq_Exp_Attr']
        + actual_cashflow_updated['Maint_Exp_Attr']
        + actual_cashflow_updated['Acq_Exp_N_Attr']
        + actual_cashflow_updated['Maint_Exp_N_Attr']
        + actual_cashflow_updated['Claims']
    )

    # Reset index and add additional columns
    actual_cashflow_updated = actual_cashflow_updated.reset_index()
    actual_cashflow_updated['Run_ID'] = filler_ids[:len(actual_cashflow_updated)]
    actual_cashflow_updated['Actual_Cashflow_ID'] = filler_ids[:len(actual_cashflow_updated)]
    actual_cashflow_updated['Active_Flag'] = active_flags[:len(actual_cashflow_updated)]
    actual_cashflow_updated['Created_By'] = created_bys[:len(actual_cashflow_updated)]
    actual_cashflow_updated['Created_Date'] = created_dates[:len(actual_cashflow_updated)]
    actual_cashflow_updated['Modified_By'] = modified_bys[:len(actual_cashflow_updated)]
    actual_cashflow_updated['Modified_Date'] = modified_dates[:len(actual_cashflow_updated)]

    # Final result
    actual_cashflow = actual_cashflow_updated
    
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
    
    Rec_BEL_updated = Rec_BEL.copy()
    risk_adjust = actual_cashflow_updated['Claims'] * risk_adjst_rate

    # Initialize temporary variable
    temp = 0
    
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
    
    if isinstance(discount_rate, (pd.Series, pd.DataFrame)):
        discount_rate = discount_rate.astype(float)

    # Update the DataFrame with vectorized operations
    for i in Rec_BEL_updated.index:
        # 1. Update 'Opening'
        Rec_BEL_updated.at[i, 'Opening'] = temp

        # 2. Update 'NewBusiness'
        Rec_BEL_updated.at[i, 'NewBusiness'] = (
            - (PV_Prem + PV_RenComm + PV_DirExpen + PV_Claims + PV_DirAqis) if i == 0 else 0
        )

        # 3. Set 'Assump' to 0
        Rec_BEL_updated.at[i, 'Assump'] = 0

        # 4. Update 'ExpInflow'
        Rec_BEL_updated.at[i, 'ExpInflow'] = actual_cashflow_updated.at[i, 'Premiums']

        # 5. Update 'ExpOutFlow'
        Rec_BEL_updated.at[i, 'ExpOutFlow'] = (
            actual_cashflow_updated.at[i, 'Acquisition_Comm'] +
            actual_cashflow_updated.at[i, 'Renewal_Comm'] +
            actual_cashflow_updated.at[i, 'Acq_Exp_Attr'] +
            actual_cashflow_updated.at[i, 'Maint_Exp_Attr'] +
            actual_cashflow_updated.at[i, 'Claims']
        )

        if isinstance(discount_rate, (pd.Series, pd.DataFrame)):
            rate = discount_rate.at[i]  # Extract value for the specific index
        else:
            rate = float(discount_rate) # Use the same rate for all rows

        # 6. Update 'FinExp'
        Rec_BEL_updated.at[i, 'FinExp'] = rate * (
            Rec_BEL_updated.at[i, 'Opening'] +
            Rec_BEL_updated.at[i, 'NewBusiness'] +
            actual_cashflow_updated.at[i, 'Premiums'] +
            actual_cashflow_updated.at[i, 'Acquisition_Comm'] +
            actual_cashflow_updated.at[i, 'Renewal_Comm'] +
            actual_cashflow_updated.at[i, 'Acq_Exp_Attr']
        )
        # 7 & 8. Set 'Changes_Exp' and 'Changes_Rel' to 0
        Rec_BEL_updated.at[i, 'Changes_Exp'] = 0
        Rec_BEL_updated.at[i, 'Changes_Rel'] = 0

        # 9. Update 'Closes'
        Rec_BEL_updated.at[i, 'Closes'] = (
            Rec_BEL_updated.at[i, 'Opening'] +
            Rec_BEL_updated.at[i, 'NewBusiness'] +
            Rec_BEL_updated.at[i, 'Assump'] +
            Rec_BEL_updated.at[i, 'ExpInflow'] +
            Rec_BEL_updated.at[i, 'ExpOutFlow'] +
            Rec_BEL_updated.at[i, 'FinExp'] +
            Rec_BEL_updated.at[i, 'Changes_Exp'] +
            Rec_BEL_updated.at[i, 'Changes_Rel']
        )

        # Update temporary variable
        temp = Rec_BEL_updated.at[i, 'Closes']

    # Reset index
    Rec_BEL_updated.reset_index(inplace=True)

    # Prepare the final DataFrame
    rec_bel = pd.DataFrame({
        'Rec_BEL_ID': filler_ids[:len(Rec_BEL_updated)],
        'Run_ID': filler_ids[:len(Rec_BEL_updated)],
        'index': Rec_BEL_updated['index'],
        'Opening': Rec_BEL_updated['Opening'],
        'NewBusiness': Rec_BEL_updated['NewBusiness'],
        'Assump': Rec_BEL_updated['Assump'],
        'ExpInflow': Rec_BEL_updated['ExpInflow'],
        'ExpOutFlow': Rec_BEL_updated['ExpOutFlow'],
        'FinExp': Rec_BEL_updated['FinExp'],
        'Changes_Exp': Rec_BEL_updated['Changes_Exp'],
        'Changes_Rel': Rec_BEL_updated['Changes_Rel'],
        'Closes': Rec_BEL_updated['Closes'],
        'Active_Flag': active_flags[:len(Rec_BEL_updated)],
        'Created_By': created_bys[:len(Rec_BEL_updated)],
        'Created_Date': created_dates[:len(Rec_BEL_updated)],
        'Modified_By': modified_bys[:len(Rec_BEL_updated)],
        'Modified_Date': modified_dates[:len(Rec_BEL_updated)],
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

    # Create a copy for updates
    Rec_RA_updated = Rec_RA.copy()

    # Initialize 'Opening' to temp (assuming temp is initially set to 0)
    temp = 0
    Rec_RA_updated['Opening'] = temp

    # Set 'NewBusiness' and 'Assump' to 0 or -PV_RiskAdj where needed
    Rec_RA_updated['NewBusiness'] = [-PV_RiskAdj if i == 0 else 0 for i in range(len(Rec_RA_updated))]
    Rec_RA_updated['Assump'] = 0

    # Set 'ExpInflow' and 'ExpOutFlow' to 0
    Rec_RA_updated['ExpInflow'] = 0
    Rec_RA_updated['ExpOutFlow'] = 0

    # Calculate 'FinExp' using a vectorized operation
    Rec_RA_updated['FinExp'] = discount_rate * (Rec_RA_updated['Opening'] + Rec_RA_updated['NewBusiness'])

    # Set 'Changes_Exp' to 0
    Rec_RA_updated['Changes_Exp'] = 0

    # Set 'Changes_Rel' based on risk_adjust
    Rec_RA_updated['Changes_Rel'] = risk_adjust

    # Calculate 'Closes' using a vectorized operation
    Rec_RA_updated['Closes'] = (
        Rec_RA_updated['Opening'] + Rec_RA_updated['NewBusiness'] + Rec_RA_updated['Assump'] + 
        Rec_RA_updated['ExpInflow'] + Rec_RA_updated['ExpOutFlow'] + Rec_RA_updated['FinExp'] + 
        Rec_RA_updated['Changes_Exp'] + Rec_RA_updated['Changes_Rel']
    )

    # Update temp for future iterations (though this might be redundant as it's not used further)
    temp = Rec_RA_updated['Closes'].iloc[-1]

    # Reset index (optional based on your needs)
    Rec_RA_updated = Rec_RA_updated.reset_index()

    # Construct final DataFrame
    rec_ra = pd.DataFrame({
        'Rec_RA_ID': filler_ids[:len(Rec_RA_updated)],
        'Run_ID': filler_ids[:len(Rec_RA_updated)],
        'index': Rec_RA_updated['index'],
        'Opening': Rec_RA_updated['Opening'],
        'NewBusiness': Rec_RA_updated['NewBusiness'],
        'Assump': Rec_RA_updated['Assump'],
        'ExpInflow': Rec_RA_updated['ExpInflow'],
        'ExpOutFlow': Rec_RA_updated['ExpOutFlow'],
        'FinExp': Rec_RA_updated['FinExp'],
        'Changes_Exp': Rec_RA_updated['Changes_Exp'],
        'Changes_Rel': Rec_RA_updated['Changes_Rel'],
        'Closes': Rec_RA_updated['Closes'],
        'Active_Flag': active_flags[:len(Rec_RA_updated)],
        'Created_By': created_bys[:len(Rec_RA_updated)],
        'Created_Date': created_dates[:len(Rec_RA_updated)],
        'Modified_By': modified_bys[:len(Rec_RA_updated)],
        'Modified_Date': modified_dates[:len(Rec_RA_updated)]
    })

    
    # Initialize Rec_CSM dataframe
    Rec_CSM = pd.DataFrame({
        'Opening': Opening,
        'NewBusiness': NewBusiness,
        'Assump': Assump,
        'ExpInflow': ExpInflow,
        'ExpOutFlow': ExpOutFlow,
        'FinExp': FinExp,
        'Changes_Exp': Changes_Exp,
        'Changes_Rel': Changes_Rel,
        'Closes': Closes
    })

    # Extend Rec_CSM with Loss_Comp if CSM_Init <= 0
    if CSM_Init <= 0:
        Rec_CSM['Loss_Comp'] = Loss_Comp

    # Prepare updated DataFrame
    Rec_CSM_updated = Rec_CSM.copy()
    if CSM_Init > 0:
        Rec_CSM_updated['Loss_Comp'] = [0] * len(Rec_CSM)

    # Initialize temp and loss_comp variables
    temp = 0
    loss_comp = CSM_Init < 0

    # Precompute sum_coverageUni for performance
    sum_coverageUni = coverage_uni_recon_updated['Opening'].iloc[i:].sum()

    if isinstance(CSM_ret_Rate, (pd.Series, pd.DataFrame)):
        CSM_ret_Rate = CSM_ret_Rate.astype(float)

    # Loop through rows with index-based iteration
    for i in Rec_CSM_updated.index:
        # 1. Update Opening
        Rec_CSM_updated.at[i, 'Opening'] = temp

        # 2. Update NewBusiness
        if i == 0:
            Rec_CSM_updated.at[i, 'NewBusiness'] = CSM_Init
        else:
            Rec_CSM_updated.at[i, 'NewBusiness'] = 0

        # 3-5. Assign zeros to columns
        for col in ['Assump', 'ExpInflow', 'ExpOutFlow']:
            Rec_CSM_updated.at[i, col] = 0
            
        if isinstance(CSM_ret_Rate, (pd.Series, pd.DataFrame)):
            rate = CSM_ret_Rate.at[i]  # Extract value for the specific index
        else:
            rate = float(CSM_ret_Rate) 
            
        # 6. Update FinExp
        Rec_CSM_updated.at[i, 'FinExp'] = rate * (
            Rec_CSM_updated.at[i, 'Opening'] +
            Rec_CSM_updated.at[i, 'NewBusiness']
        )

        # 7. Update Changes_Exp
        Rec_CSM_updated.at[i, 'Changes_Exp'] = 0

        # 8. Update Changes_Rel
        Changes_Rel_denom = (
            Rec_CSM_updated.at[i, 'Opening'] +
            Rec_CSM_updated.at[i, 'NewBusiness'] +
            Rec_CSM_updated.at[i, 'Assump'] +
            Rec_CSM_updated.at[i, 'ExpInflow'] +
            Rec_CSM_updated.at[i, 'ExpOutFlow'] +
            Rec_CSM_updated.at[i, 'FinExp']
        )
        Rec_CSM_updated.at[i, 'Changes_Rel'] = -(
            Changes_Rel_denom *
            coverage_uni_recon_updated.at[i, 'Opening'] /
            sum_coverageUni
        )

        # 9. Update Closes
        Rec_CSM_updated.at[i, 'Closes'] = sum(
            Rec_CSM_updated.loc[i, [
                'Opening', 'NewBusiness', 'Assump',
                'ExpInflow', 'ExpOutFlow', 'FinExp',
                'Changes_Exp', 'Changes_Rel'
            ]]
        )

        # Handle Loss_Comp logic
        if loss_comp:
            loss_comp_value = Rec_CSM_updated.at[i, 'Loss_Comp']
            closes_value = Rec_CSM_updated.at[i, 'Closes']

            if -loss_comp_value > closes_value:
                Rec_CSM_updated.at[i, 'Loss_Comp'] += closes_value
                Rec_CSM_updated.at[i, 'Closes'] = 0
                temp = Rec_CSM_updated.at[i, 'Loss_Comp']
            else:
                Rec_CSM_updated.at[i, 'Closes'] += loss_comp_value
                Rec_CSM_updated.at[i, 'Loss_Comp'] = 0
                temp = Rec_CSM_updated.at[i, 'Closes']
        else:
            temp = Rec_CSM_updated.at[i, 'Closes']

    # Reset index and create final DataFrame
    Rec_CSM_updated.reset_index(inplace=True)

    rec_csm = pd.DataFrame({
        'Rec_CSM_ID': filler_ids[:len(Rec_CSM_updated)],
        'Run_ID': filler_ids[:len(Rec_CSM_updated)],
        'index': Rec_CSM_updated['index'],
        'Opening': Rec_CSM_updated['Opening'],
        'NewBusiness': Rec_CSM_updated['NewBusiness'],
        'Assump': Rec_CSM_updated['Assump'],
        'ExpInflow': Rec_CSM_updated['ExpInflow'],
        'ExpOutFlow': Rec_CSM_updated['ExpOutFlow'],
        'FinExp': Rec_CSM_updated['FinExp'],
        'Changes_Exp': Rec_CSM_updated['Changes_Exp'],
        'Changes_Rel': Rec_CSM_updated['Changes_Rel'],
        'Closes': Rec_CSM_updated['Closes'],
        'Loss_Comp': Rec_CSM_updated.get('Loss_Comp', [0] * len(Rec_CSM_updated)),
        'Active_Flag': active_flags[:len(Rec_CSM_updated)],
        'Created_By': created_bys[:len(Rec_CSM_updated)],
        'Created_Date': created_dates[:len(Rec_CSM_updated)],
        'Modified_By': modified_bys[:len(Rec_CSM_updated)],
        'Modified_Date': modified_dates[:len(Rec_CSM_updated)]
    })

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
    
    Liab_init_reco = Liab_init_reco.reset_index()
    
    liability_init_rec = pd.DataFrame() 
    liability_init_rec['Liability_Init_Rec_ID'] = filler_ids[:len(Liab_init_reco)]
    liability_init_rec['Run_ID'] = filler_ids[:len(Liab_init_reco)]
    liability_init_rec['index'] = Liab_init_reco['index']
    liability_init_rec['PV_Prem'] = Liab_init_reco['PV_Prem']
    liability_init_rec['PV_RenComm'] = Liab_init_reco['PV_RenComm']
    liability_init_rec['PV_Claims'] = Liab_init_reco['PV_Claims']
    liability_init_rec['PV_DirAqis'] = Liab_init_reco['PV_DirAqis']
    liability_init_rec['PV_RiskAdj'] = Liab_init_reco['PV_RiskAdj']
    liability_init_rec['Total'] = Liab_init_reco['Total']
    liability_init_rec['CSM_Init'] = Liab_init_reco['CSM_Init']
    liability_init_rec['Liab_Init'] = Liab_init_reco['Liab_Init']
    liability_init_rec['Active_Flag'] = active_flags[:len(Liab_init_reco)]
    liability_init_rec['Created_By'] = created_bys[:len(Liab_init_reco)]
    liability_init_rec['Created_Date'] = created_dates[:len(Liab_init_reco)]
    liability_init_rec['Modified_By'] = modified_bys[:len(Liab_init_reco)]
    liability_init_rec['Modified_Date'] = modified_dates[:len(Liab_init_reco)]
    
    
    Rec_TotContLiab_up = pd.DataFrame({
        'Opening': Rec_BEL_updated['Opening'] + Rec_RA_updated['Opening'] + Rec_CSM_updated['Opening'],
        'NewBusiness': Rec_BEL_updated['NewBusiness'] + Rec_RA_updated['NewBusiness'] + Rec_CSM_updated['NewBusiness'],
        'Assump': Rec_BEL_updated['Assump'] + Rec_RA_updated['Assump'] + Rec_CSM_updated['Assump'],
        'ExpInflow': Rec_BEL_updated['ExpInflow'] + Rec_RA_updated['ExpInflow'] + Rec_CSM_updated['ExpInflow'],
        'ExpOutFlow': Rec_BEL_updated['ExpOutFlow'] + Rec_RA_updated['ExpOutFlow'] + Rec_CSM_updated['ExpOutFlow'],
        'FinExp': Rec_BEL_updated['FinExp'] + Rec_RA_updated['FinExp'] + Rec_CSM_updated['FinExp'],
        'Changes_Exp': Rec_BEL_updated['Changes_Exp'] + Rec_RA_updated['Changes_Exp'] + Rec_CSM_updated['Changes_Exp'],
        'Changes_Rel': Rec_BEL_updated['Changes_Rel'] + Rec_RA_updated['Changes_Rel'] + Rec_CSM_updated['Changes_Rel'],
        'Closes': Rec_BEL_updated['Closes'] + Rec_RA_updated['Closes'] + Rec_CSM_updated['Closes']
    }).reset_index()

    rec_totcontliab = pd.DataFrame({
        'Rec_TotContLiab_ID': filler_ids[:len(Rec_TotContLiab_up)],
        'Run_ID': filler_ids[:len(Rec_TotContLiab_up)],
        'index': Rec_TotContLiab_up['index'],
        'Opening': Rec_TotContLiab_up['Opening'],
        'NewBusiness': Rec_TotContLiab_up['NewBusiness'],
        'Assump': Rec_TotContLiab_up['Assump'],
        'ExpInflow': Rec_TotContLiab_up['ExpInflow'],
        'ExpOutFlow': Rec_TotContLiab_up['ExpOutFlow'],
        'FinExp': Rec_TotContLiab_up['FinExp'],
        'Changes_Exp': Rec_TotContLiab_up['Changes_Exp'],
        'Changes_Rel': Rec_TotContLiab_up['Changes_Rel'],
        'Closes': Rec_TotContLiab_up['Closes'],
        'Active_Flag': active_flags[:len(Rec_TotContLiab_up)],
        'Created_By': created_bys[:len(Rec_TotContLiab_up)],
        'Created_Date': created_dates[:len(Rec_TotContLiab_up)],
        'Modified_By': modified_bys[:len(Rec_TotContLiab_up)],
        'Modified_Date': modified_dates[:len(Rec_TotContLiab_up)]
    })

    Rec_AcqExpMor = pd.DataFrame(
        {'Opening': Opening,
         'NewAcqExp': NewAcqExp,
         'AccIntr': AccIntr,
         'AmorExp': AmorExp,
         'Closes': Closes
         })
    
    # Precompute values outside the loop
    coverage_uni_cumsum = coverage_uni_recon_updated['Opening'][::-1].cumsum()[::-1]

    # Initialize the result DataFrame with zeroed columns
    Rec_AcqExpMor_up = pd.DataFrame(index=Rec_AcqExpMor.index)
    Rec_AcqExpMor_up['Opening'] = 0
    Rec_AcqExpMor_up['NewAcqExp'] = -(
        actual_cashflow_updated['Acquisition_Comm'] + actual_cashflow_updated['Acq_Exp_Attr']
    )
    Rec_AcqExpMor_up['AccIntr'] = 0
    Rec_AcqExpMor_up['AmorExp'] = 0
    Rec_AcqExpMor_up['Closes'] = 0

    temp = 0
    
    if isinstance(discount_rate, (pd.Series, pd.DataFrame)):
        discount_rate = discount_rate.astype(float)

        
    for i in range(len(Rec_AcqExpMor)):
        # 1. Update Opening
        Rec_AcqExpMor_up.loc[i, 'Opening'] = temp
        
        if isinstance(discount_rate, (pd.Series, pd.DataFrame)):
            rate = discount_rate.at[i]  # Extract value for the specific index
        else:
            rate = float(discount_rate)
            
        # 2. AccIntr calculation
        Rec_AcqExpMor_up.loc[i, 'AccIntr'] = rate * (
            Rec_AcqExpMor_up.loc[i, 'Opening'] + Rec_AcqExpMor_up.loc[i, 'NewAcqExp']
        )
        
        # 3. AmorExp calculation
        total_opening = Rec_AcqExpMor_up.loc[i, 'Opening'] + Rec_AcqExpMor_up.loc[i, 'NewAcqExp'] + Rec_AcqExpMor_up.loc[i, 'AccIntr']
        Rec_AcqExpMor_up.loc[i, 'AmorExp'] = -(
            total_opening * coverage_uni_recon_updated.loc[i, 'Opening'] / coverage_uni_cumsum[i]
        )
        
        # 4. Closes calculation
        Rec_AcqExpMor_up.loc[i, 'Closes'] = (
            Rec_AcqExpMor_up.loc[i, 'Opening'] +
            Rec_AcqExpMor_up.loc[i, 'NewAcqExp'] +
            Rec_AcqExpMor_up.loc[i, 'AccIntr'] +
            Rec_AcqExpMor_up.loc[i, 'AmorExp']
        )
        
        # Update temp for the next iteration
        temp = Rec_AcqExpMor_up.loc[i, 'Closes']

    # Add metadata columns
    rec_acqexpmor_up = pd.DataFrame({
        'Rec_AcqExpMor_ID': filler_ids[:len(Rec_AcqExpMor_up)],
        'Run_ID': filler_ids[:len(Rec_AcqExpMor_up)],
        'index': Rec_AcqExpMor_up.index,
        'Opening': Rec_AcqExpMor_up['Opening'],
        'NewAcqExp': Rec_AcqExpMor_up['NewAcqExp'],
        'AccIntr': Rec_AcqExpMor_up['AccIntr'],
        'AmorExp': Rec_AcqExpMor_up['AmorExp'],
        'Closes': Rec_AcqExpMor_up['Closes'],
        'Active_Flag': active_flags[:len(Rec_AcqExpMor_up)],
        'Created_By': created_bys[:len(Rec_AcqExpMor_up)],
        'Created_Date': created_dates[:len(Rec_AcqExpMor_up)],
        'Modified_By': modified_bys[:len(Rec_AcqExpMor_up)],
        'Modified_Date': modified_dates[:len(Rec_AcqExpMor_up)],
    })

    Stat_Profloss = pd.DataFrame(
        {
            'Rel_CSM': Rel_CSM,
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
        }
    )

    # Create a copy of the DataFrame
    Stat_Profloss_up = Stat_Profloss.copy()

    # Perform vectorized updates
    Stat_Profloss_up['Rel_CSM'] = -Rec_CSM_updated['Changes_Rel'].values
    Stat_Profloss_up['Rel_RA'] = -Rec_RA_updated['Changes_Rel'].values
    Stat_Profloss_up['Exp_Claim'] = -actual_cashflow_updated['Claims'].values
    Stat_Profloss_up['Exp_Expen'] = -(actual_cashflow_updated['Renewal_Comm'] + actual_cashflow_updated['Maint_Exp_Attr']).values
    Stat_Profloss_up['Rec_AcqCasFl'] = -Rec_AcqExpMor_up['AmorExp'].values

    Stat_Profloss_up['Ins_SerRev'] = (
        Stat_Profloss_up['Rel_CSM'] +
        Stat_Profloss_up['Rel_RA'] +
        Stat_Profloss_up['Exp_Claim'] +
        Stat_Profloss_up['Exp_Expen'] +
        Stat_Profloss_up['Rec_AcqCasFl']
    )

    Stat_Profloss_up['Claim_Incur'] = actual_cashflow_updated['Claims'].values
    Stat_Profloss_up['Exp_Incur'] = (actual_cashflow_updated['Renewal_Comm'] + actual_cashflow_updated['Maint_Exp_Attr']).values
    Stat_Profloss_up['Amor_AcqCasFlo'] = Rec_AcqExpMor_up['AmorExp'].values

    Stat_Profloss_up['Ins_SerExp'] = (
        Stat_Profloss_up['Claim_Incur'] +
        Stat_Profloss_up['Exp_Incur'] +
        Stat_Profloss_up['Amor_AcqCasFlo']
    )

    Stat_Profloss_up['Other_Exp'] = (
        actual_cashflow_updated['Acq_Exp_N_Attr'] + 
        actual_cashflow_updated['Maint_Exp_N_Attr']
    ).values

    Stat_Profloss_up['Inv_income'] = (
        asset_ret_rate * (
            Rec_TotContLiab_up['Opening'] + 
            Rec_TotContLiab_up['NewBusiness']
        ).values +
        asset_ret_rate * (
            actual_cashflow_updated['Premiums'] +
            actual_cashflow_updated['Acquisition_Comm'] +
            actual_cashflow_updated['Renewal_Comm'] +
            actual_cashflow_updated['Acq_Exp_Attr']
        ).values
    )

    Stat_Profloss_up['Ins_FinExp'] = -Rec_TotContLiab_up['FinExp'].values
    Stat_Profloss_up['Fin_GainLoss'] = Stat_Profloss_up['Inv_income'] + Stat_Profloss_up['Ins_FinExp']
    Stat_Profloss_up['Prof_loss'] = (
        Stat_Profloss_up['Ins_SerRev'] +
        Stat_Profloss_up['Ins_SerExp'] +
        Stat_Profloss_up['Other_Exp'] +
        Stat_Profloss_up['Fin_GainLoss']
    )

    # Adding additional columns
    Stat_Profloss_up['Stat_Profloss_ID'] = filler_ids[:len(Stat_Profloss_up)]
    Stat_Profloss_up['Run_ID'] = filler_ids[:len(Stat_Profloss_up)]
    Stat_Profloss_up['index'] = range(len(Stat_Profloss_up))
    Stat_Profloss_up['Active_Flag'] = active_flags[:len(Stat_Profloss_up)]
    Stat_Profloss_up['Created_By'] = created_bys[:len(Stat_Profloss_up)]
    Stat_Profloss_up['Created_Date'] = created_dates[:len(Stat_Profloss_up)]
    Stat_Profloss_up['Modified_By'] = modified_bys[:len(Stat_Profloss_up)]
    Stat_Profloss_up['Modified_Date'] = modified_dates[:len(Stat_Profloss_up)]

    # Assigning updated DataFrame
    stat_profloss_up = Stat_Profloss_up
    
    conf_id = str(uuid.uuid4())
     
    run = pd.DataFrame()
    run['Run_ID'] = filler_ids[:len(coverage_uni_recon_updated)]
    run['Run_Name']= [run_name] * len(coverage_uni_recon_updated)
    run['Conf_ID'] = [conf_id] * len(coverage_uni_recon_updated)
    run['Reporting_Date']=[datetime.now()] * len(coverage_uni_recon_updated)
    run['Active_Flag'] = active_flags[:len(coverage_uni_recon_updated)]
    run['Created_By'] = created_bys[:len(coverage_uni_recon_updated)]
    run['Created_Date'] = created_dates[:len(coverage_uni_recon_updated)]
    run['Modified_By'] = modified_bys[:len(coverage_uni_recon_updated)]
    run['Modified_Date'] = modified_dates[:len(coverage_uni_recon_updated)]    
    
    run_input = df_temp
    run_input['Run_ID'] = filler_ids[:len(df_temp)]
    run_input['Run_Input_ID'] = filler_ids[:len(df_temp)]
    run_input['Active_Flag'] = active_flags[:len(df_temp)]
    run_input['Created_By'] = created_bys[:len(df_temp)]
    run_input['Created_Date'] = created_dates[:len(df_temp)]
    run_input['Modified_By'] = modified_bys[:len(df_temp)]
    run_input['Modified_Date'] = modified_dates[:len(df_temp)]
    
    return run,run_input,coverage_uni_recon, actual_cashflow, liability_init_rec, rec_bel, rec_ra, rec_csm, rec_totcontliab, rec_acqexpmor_up , stat_profloss_up

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
    Total_years = df_temp['Total_years'].astype(int)
    discount_rate = df_temp['discount_rate'][0].astype(float).item()
    asset_ret_rate = df_temp['asset_ret_rate'][0].astype(float).item()
    risk_adjst_rate = df_temp['risk_adjst_rate'][0].astype(float).item()
    mortality = df_temp['mortality']
    lapse = df_temp['lapse']

    # Create a DataFrame from the user input
    Opening = [0] * Total_years
    Deaths = [0] * Total_years
    Closes = [0] * Total_years
    Premiums = [0] * Total_years
    Acquisition_Comm = [0] * Total_years
    Renewal_Comm = [0] * Total_years
    Acq_Exp_Attr = [0] * Total_years
    Maint_Exp_Attr = [0] * Total_years
    Acq_Exp_N_Attr = [0] * Total_years
    Maint_Exp_N_Attr = [0] * Total_years
    Claims = [0] * Total_years
    Total_Net_CFs = [0] * Total_years
    ExpInflow = [0] * Total_years
    ExpOutFlow = [0] * Total_years
    FinExp = [0] * Total_years
    Changes_Exp = [0] * Total_years
    Gross_Prem = [0] * Total_years
    Invest_inc = [0] * Total_years
    Total_Income = [0] * Total_years
    Claim_Incur = [0] * Total_years
    Exp_Incur = [0] * Total_years
    Amor_AcqCasFlo = [0] * Total_years
    Chnge_Insu_Liab = [0] * Total_years
    Total_Expenses = [0] * Total_years
    Prof_loss = [0] * Total_years
    NewAcqExp = [0] * Total_years
    AccIntr = [0] * Total_years
    AmorExp = [0] * Total_years


    coverage_uni_recon = pd.DataFrame({
        'Opening': Opening,
        'Deaths': Deaths,
        'Lapses': lapse,
        'Closes': Closes
    })

    # Create a copy of the original DataFrame
    coverage_uni_recon_updated = coverage_uni_recon.copy()

    # Initialize the Opening column
    coverage_uni_recon_updated['Opening'] = 0
    coverage_uni_recon_updated.loc[0, 'Opening'] = Num_Policies

    # Compute Deaths, Lapses, and Closes iteratively for dependency handling
    temp = Num_Policies
    for i in range(len(coverage_uni_recon_updated)):
        # Deaths and Lapses
        deaths = mortality[i] * temp
        lapses = lapse[i] * temp

        # Update the DataFrame
        coverage_uni_recon_updated.at[i, 'Deaths'] = deaths
        coverage_uni_recon_updated.at[i, 'Lapses'] = lapses

        # Calculate Closes
        closes = max(0, temp - deaths - lapses)
        coverage_uni_recon_updated.at[i, 'Closes'] = closes

        # Update temp and Opening for the next iteration
        if i < len(coverage_uni_recon_updated) - 1:
            coverage_uni_recon_updated.at[i + 1, 'Opening'] = closes
            temp = closes

    actual_cashflow = pd.DataFrame({
        'Premiums': Premiums,
        'Acquisition_Comm': Acquisition_Comm,
        'Renewal_Comm': Renewal_Comm,
        'Acq_Exp_Attr': Acq_Exp_Attr,
        'Maint_Exp_Attr': Maint_Exp_Attr,
        'Acq_Exp_N_Attr': Acq_Exp_N_Attr,
        'Maint_Exp_N_Attr': Maint_Exp_N_Attr,
        'Claims': Claims,
        'Total_Net_CFs': Total_Net_CFs
    })
    
    # cash flow calculations

    # Create a copy of the original DataFrame
    actual_cashflow_updated = actual_cashflow.copy()

    # Calculate `Premiums` vectorized
    actual_cashflow_updated['Premiums'] = (
        (sum_assured * np.array(prem_rate_per1000) / 1000 + policy_fees)
        * coverage_uni_recon_updated['Opening']
    )

    # Calculate `Acquisition_Comm` vectorized
    actual_cashflow_updated['Acquisition_Comm'] = 0
    actual_cashflow_updated.loc[0, 'Acquisition_Comm'] = (
        -policy_Init_comm * actual_cashflow_updated.loc[0, 'Premiums']
    )

    # Calculate `Renewal_Comm` vectorized
    actual_cashflow_updated['Renewal_Comm'] = (
        -policy_yearly_comm * actual_cashflow_updated['Premiums']
    )
    actual_cashflow_updated.loc[0, 'Renewal_Comm'] = 0  # No renewal commission for the first row

    # Calculate `Acq_Exp_Attr` vectorized
    actual_cashflow_updated['Acq_Exp_Attr'] = 0
    actual_cashflow_updated.loc[0, 'Acq_Exp_Attr'] = (
        -acq_direct_expenses * coverage_uni_recon_updated.loc[0, 'Opening']
    )

    # Calculate `Maint_Exp_Attr` vectorized
    actual_cashflow_updated['Maint_Exp_Attr'] = (
        -main_direct_expenses * coverage_uni_recon_updated['Opening']
    )

    # Calculate `Acq_Exp_N_Attr` vectorized
    actual_cashflow_updated['Acq_Exp_N_Attr'] = 0
    actual_cashflow_updated.loc[0, 'Acq_Exp_N_Attr'] = (
        -acq_indirect_expense * coverage_uni_recon_updated.loc[0, 'Opening']
    )

    # Calculate `Maint_Exp_N_Attr` vectorized
    actual_cashflow_updated['Maint_Exp_N_Attr'] = (
        -main_indirect_expenses * coverage_uni_recon_updated['Opening']
    )

    # Calculate `Claims` vectorized
    actual_cashflow_updated['Claims'] = (
        -sum_assured * coverage_uni_recon_updated['Deaths']
    )

    # Calculate `Total_Net_CFs` vectorized
    actual_cashflow_updated['Total_Net_CFs'] = (
        actual_cashflow_updated['Premiums']
        + actual_cashflow_updated['Acquisition_Comm']
        + actual_cashflow_updated['Renewal_Comm']
        + actual_cashflow_updated['Acq_Exp_Attr']
        + actual_cashflow_updated['Maint_Exp_Attr']
        + actual_cashflow_updated['Acq_Exp_N_Attr']
        + actual_cashflow_updated['Maint_Exp_N_Attr']
        + actual_cashflow_updated['Claims']
    )
    
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
        
    Rec_BEL = pd.DataFrame({
        'Opening': Opening,
        'ExpInflow': ExpInflow,
        'ExpOutFlow': ExpOutFlow,
        'FinExp': FinExp,
        'Changes_Exp': Changes_Exp,
        'Closes': Closes
    })

    # Reconciliation of Best Estimate Liabilities (BEL)

    # Create a copy of the original DataFrame
    Rec_BEL_updated = Rec_BEL.copy()

    # Initialize `temp` for the first iteration
    Rec_BEL_updated['Opening'].iloc[0] = Liab_Init

    # Calculate `ExpInflow` directly
    Rec_BEL_updated['ExpInflow'] = actual_cashflow_updated['Premiums']

    # Calculate `ExpOutFlow` directly
    Rec_BEL_updated['ExpOutFlow'] = (
        actual_cashflow_updated['Acquisition_Comm'] +
        actual_cashflow_updated['Renewal_Comm'] +
        actual_cashflow_updated['Acq_Exp_Attr'] +
        actual_cashflow_updated['Maint_Exp_Attr'] +
        actual_cashflow_updated['Claims']
    )

    # Calculate `FinExp` directly
    Rec_BEL_updated['FinExp'] = (
        discount_rate * Rec_BEL_updated['Opening'] +
        discount_rate * (
            actual_cashflow_updated['Premiums'] +
            actual_cashflow_updated['Acquisition_Comm'] +
            actual_cashflow_updated['Renewal_Comm'] +
            actual_cashflow_updated['Acq_Exp_Attr']
        )
    )

    # Set `Changes_Exp` to zero
    Rec_BEL_updated['Changes_Exp'] = 0

    # Iteratively calculate `Opening` and `Closes` using a cumulative approach
    for i in range(1, len(Rec_BEL_updated)):
        Rec_BEL_updated['Opening'].iloc[i] = Rec_BEL_updated['Closes'].iloc[i - 1]

    Rec_BEL_updated['Closes'] = (
        Rec_BEL_updated['Opening'] +
        Rec_BEL_updated['ExpInflow'] +
        Rec_BEL_updated['ExpOutFlow'] +
        Rec_BEL_updated['FinExp'] +
        Rec_BEL_updated['Changes_Exp']
    )

    Rec_AcqExpMor = pd.DataFrame(
        {'Opening': Opening,
         'NewAcqExp': NewAcqExp,
         'AccIntr': AccIntr,
         'AmorExp': AmorExp,
         'Closes': Closes
         })
    
    # Create a copy of the original DataFrame
    Rec_AcqExpMor_up = Rec_AcqExpMor.copy()

    # Initialize the `temp` variable to store the cumulative `Closes` value
    temp = 0

    # Precompute `NewAcqExp` vectorized
    Rec_AcqExpMor_up['NewAcqExp'] = -(actual_cashflow_updated['Acquisition_Comm'] + 
                                    actual_cashflow_updated['Acq_Exp_Attr'])

    # Precompute `AccIntr` vectorized
    Rec_AcqExpMor_up['AccIntr'] = discount_rate * (temp + Rec_AcqExpMor_up['NewAcqExp'])

    # Precompute `sum_coverageUni` for all rows
    sum_coverageUni = coverage_uni_recon_updated['Opening'].iloc[::-1].cumsum()[::-1]

    # Calculate `AmorExp` vectorized
    Rec_AcqExpMor_up['AmorExp'] = -(((temp + Rec_AcqExpMor_up['NewAcqExp'] + Rec_AcqExpMor_up['AccIntr']) *
                                    coverage_uni_recon_updated['Opening']) / sum_coverageUni)

    # Calculate `Closes` vectorized
    Rec_AcqExpMor_up['Closes'] = (temp +
                                Rec_AcqExpMor_up['NewAcqExp'] +
                                Rec_AcqExpMor_up['AccIntr'] +
                                Rec_AcqExpMor_up['AmorExp'])

    # Update `Opening` with the previous `Closes` value iteratively
    Rec_AcqExpMor_up['Opening'] = Rec_AcqExpMor_up['Closes'].shift(1, fill_value=0)

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

    # Create a copy of the original DataFrame
    Stat_Profloss_up = Stat_Profloss.copy()

    # Update columns using vectorized operations
    Stat_Profloss_up['Gross_Prem'] = actual_cashflow_updated['Premiums']

    Stat_Profloss_up['Invest_inc'] = asset_ret_rate * Stat_Profloss_up['Gross_Prem']

    Stat_Profloss_up['Total_Income'] = Stat_Profloss_up['Gross_Prem'] + Stat_Profloss_up['Invest_inc']

    Stat_Profloss_up['Claim_Incur'] = actual_cashflow_updated['Claims']

    Stat_Profloss_up['Exp_Incur'] = (actual_cashflow_updated['Renewal_Comm'] + 
                                    actual_cashflow_updated['Maint_Exp_Attr'])

    Stat_Profloss_up['Amor_AcqCasFlo'] = Rec_AcqExpMor_up['AmorExp']

    Stat_Profloss_up['Chnge_Insu_Liab'] = (Rec_BEL_updated['Opening'] - 
                                        Rec_BEL_updated['Closes'])

    Stat_Profloss_up['Total_Expenses'] = (Stat_Profloss_up['Claim_Incur'] +
                                        Stat_Profloss_up['Exp_Incur'] +
                                        Stat_Profloss_up['Amor_AcqCasFlo'] +
                                        Stat_Profloss_up['Chnge_Insu_Liab'])

    Stat_Profloss_up['Prof_loss'] = (Stat_Profloss_up['Total_Income'] -
                                    Stat_Profloss_up['Total_Expenses'])

    # Risk adjustment calculations

    risk_adjust = actual_cashflow_updated['Claims'] * risk_adjst_rate
    print(risk_adjust.T)

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
            "comp":(output_data_GMM[t2]-output_data_ifrs[t1])[comp_cols]
            }
    return comp

def enter_details_to_db():
    input_data = pd.read_csv(r"New_Data_Random_inputs.csv",index_col=0)
    input_data1=input_data.loc[0]
    
    run , run_input,coverage_uni_recon, actual_cashflow, Liab_init_reco, Rec_BEL_updated, Rec_RA_updated, Rec_CSM_updated, Rec_TotContLiab_up, Rec_AcqExpMor_up, Stat_Profloss_up = calculate(input_data1,"run_name")
    
    send_bulk_records(run,'http://127.0.0.1:8000/api/insert/Run')
    # send_bulk_records(coverage_uni_recon,'http://127.0.0.1:8000/api/coverage-units-rec')
    # send_bulk_records(actual_cashflow,'http://127.0.0.1:8000/api/actual-cashflow')
    # send_bulk_records(Liab_init_reco,'http://127.0.0.1:8000/api/liability-init-rec')
    # send_bulk_records(Rec_BEL_updated,'http://127.0.0.1:8000/api/rec-bel-updated')
    

enter_details_to_db()
# input_data = pd.read_csv(r"New_Data_Random_inputs.csv",index_col=0)
# input_data1=input_data.loc[0]

# output_data=calculate(input_data1)