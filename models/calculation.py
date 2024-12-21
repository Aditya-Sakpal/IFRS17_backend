import uuid

from sqlalchemy import Column, String, Boolean, ForeignKey, TIMESTAMP, Date , Integer , Numeric
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from models.base import Base

class Run(Base):
    __tablename__ = "Run"
    __table_args__ = {"schema": "Calculation"}
    
    Run_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    Run_Name = Column(String(100), nullable=False)
    Conf_ID = Column(PGUUID(as_uuid=True), nullable=False)
    Reporting_Date = Column(Date, nullable=False)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), ForeignKey("UserManagement.User.User_Name"), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)
    
    user = relationship("User", back_populates="runs")

    coverage_units_rec = relationship("CoverageUnitsRec", back_populates="run", cascade="all, delete-orphan")
    
    cash_flow = relationship("CashFlow", back_populates="run", cascade="all, delete-orphan")

    liability_init_rec = relationship("Liability_Init_Rec", back_populates="run", cascade="all, delete-orphan")
    
    rec_bel_updated = relationship("Rec_Bel_Updated", back_populates="run", cascade="all, delete-orphan")
    
    run_inputs = relationship("RunInput", back_populates="run", cascade="all, delete-orphan")   

class RunInput(Base):
    __tablename__ = "Run_Input"
    __table_args__ = {"schema": "Calculation"}
    
    Run_Input_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    Run_ID = Column(PGUUID(as_uuid=True), ForeignKey("Calculation.Run.Run_ID"), nullable=False)
    Sum_Assured = Column(Numeric, nullable=False)
    Num_Policies = Column(Integer, nullable=False)
    Policy_Fees = Column(Numeric, nullable=False)
    Prem_Rate_Per1000 = Column(Numeric, nullable=False)
    Policy_Init_Com = Column(Numeric, nullable=False)
    Policy_Yrly_Com = Column(Numeric, nullable=False)
    Acq_Direct_Expenses = Column(Numeric, nullable=False)
    Acq_Indirect_Expense = Column(Numeric, nullable=False)
    Main_Direct_Expenses = Column(Numeric, nullable=False)
    Main_Indirect_Expenses = Column(Numeric, nullable=False)
    Total_Years = Column(Integer, nullable=False)
    Discount_Rate = Column(Numeric, nullable=False)
    Asset_Ret_Rate = Column(Numeric, nullable=False)
    CSM_Ret_Rate = Column(Numeric, nullable=False)
    Risk_Adjst_Rate = Column(Numeric, nullable=False)
    Mortality = Column(Numeric, nullable=False)
    Lapse = Column(Numeric, nullable=False)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), ForeignKey("UserManagement.User.User_Name"), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)
    
    user = relationship("User", back_populates="run_inputs")
    run = relationship("Run", back_populates="run_inputs")
    

class CoverageUnitsRec(Base):
    __tablename__ = "Coverage_Units_Rec"
    __table_args__ = {"schema": "Calculation"}
    
    Coverage_Units_Rec_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    Run_ID = Column(PGUUID(as_uuid=True), ForeignKey("Calculation.Run.Run_ID"), nullable=False)
    index = Column(Integer, nullable=False)
    Opening = Column(Numeric, nullable=False)
    Deaths = Column(Numeric, nullable=False)
    Lapses = Column(Numeric, nullable=False)
    Closes = Column(Numeric, nullable=False)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), ForeignKey("UserManagement.User.User_Name"), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)
    
    # Relationship with Run
    run = relationship("Run", back_populates="coverage_units_rec")

    # Relationship with User
    user = relationship("User", back_populates="coverage_units_rec")
    
    
    

class CashFlow(Base):
    __tablename__ = "Actual_Cashflow"
    __table_args__ = {"schema": "Calculation"}
    
    Actual_Cashflow_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    Run_ID = Column(PGUUID(as_uuid=True), ForeignKey("Calculation.Run.Run_ID"), nullable=False)
    index = Column(Integer, nullable=False)
    Premiums = Column(Numeric, nullable=False)
    Acquisition_Comm = Column(Numeric, nullable=False)
    Renewal_Comm = Column(Numeric, nullable=False)
    Acq_Exp_Attr = Column(Numeric, nullable=False)
    Maint_Exp_Attr = Column(Numeric, nullable=False)
    Acq_Exp_N_Attr = Column(Numeric, nullable=False)
    Maint_Exp_N_Attr = Column(Numeric, nullable=False)
    Claims = Column(Numeric, nullable=False)
    Total_Net_CFs = Column(Numeric, nullable=False)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), ForeignKey("UserManagement.User.User_Name"), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)
    
    # Relationship with Run
    run = relationship("Run", back_populates="cash_flow")
    
    # Relationship with User
    user = relationship("User", back_populates="cash_flow")
    
class Liability_Init_Rec(Base):
    __tablename__ = "Liability_Init_Rec"
    __table_args__ = {"schema": "Calculation"}
    
    Liability_Init_Rec_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    Run_ID = Column(PGUUID(as_uuid=True), ForeignKey("Calculation.Run.Run_ID"), nullable=False)
    index = Column(Integer, nullable=False)
    PV_Prem = Column(Numeric, nullable=False)
    PV_RenComm = Column(Numeric, nullable=False)
    PV_Claims = Column(Numeric, nullable=False)
    PV_DirAqis = Column(Numeric, nullable=False)
    PV_RiskAdj = Column(Numeric, nullable=False)
    Total = Column(Numeric, nullable=False)
    CSM_Init = Column(Numeric, nullable=False)
    Liab_Init = Column(Numeric, nullable=False)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), ForeignKey("UserManagement.User.User_Name"), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)
    
    # Relationship with Run
    run = relationship("Run", back_populates="liability_init_rec")
    
    # Relationship with User
    user = relationship("User", back_populates="liability_init_rec")
    
class Rec_Bel_Updated(Base):
    __tablename__ = "Rec_Bel"
    __table_args__ = {"schema": "Calculation"}
    
    Rec_Bec_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    Run_ID = Column(PGUUID(as_uuid=True), ForeignKey("Calculation.Run.Run_ID"), nullable=False)
    index = Column(Integer, nullable=False)
    Opening = Column(Numeric, nullable=False)
    NewBusiness = Column(Numeric, nullable=False)
    Assump = Column(Numeric, nullable=False)
    ExpInFlow = Column(Numeric, nullable=False)
    ExpOutFlow = Column(Numeric, nullable=False)
    FinExp = Column(Numeric, nullable=False)
    Changes_Exp = Column(Numeric, nullable=False)
    Changes_Rel = Column(Numeric, nullable=False)
    Closes = Column(Numeric, nullable=False)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), ForeignKey("UserManagement.User.User_Name"), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)
    
    # Relationship with Run
    run = relationship("Run", back_populates="rec_bel_updated")
    
    # Relationship with User
    user = relationship("User", back_populates="rec_bel_updated")
    
    