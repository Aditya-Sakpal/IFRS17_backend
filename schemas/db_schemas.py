from pydantic import BaseModel, Field, EmailStr
from datetime import date, datetime
from typing import Optional
from uuid import UUID

class CreateUserRequest(BaseModel):
    User_Name: str = Field(..., max_length=100)
    User_Group: str = Field(..., max_length=100)
    Email_ID: EmailStr = None
    User_Desc: str = None
    Active_Flag: bool = True
    Created_By: str = Field(..., max_length=100)
    
class CreateUserGroupRequest(BaseModel):
    User_Group_Name: str
    User_Group_Desc: Optional[str]
    Active_Flag: Optional[bool] = True
    Created_By: str
    
class CreateRunRequest(BaseModel):
    Run_ID: UUID
    Run_Name: str = Field(..., max_length=100)
    Conf_ID: UUID
    Reporting_Date: date
    Active_Flag: Optional[bool] = True
    Created_By: str
    Created_Date: datetime
    Modified_By: Optional[str] = None
    Modified_Date: Optional[datetime] = None
    
class CreateCoverageUnitsRecRequest(BaseModel):
    Coverage_Units_Rec_ID: UUID
    Run_ID: UUID
    index: int
    Opening: float
    Deaths: float
    Lapses: float
    Closes: float
    Active_Flag: Optional[bool] = True
    Created_By: str
    Created_Date: datetime
    Modified_By: Optional[str] = None
    Modified_Date: Optional[datetime] = None
    
    
class CreateCashFlowRequest(BaseModel):
    Actual_Cashflow_ID: UUID
    Run_ID: UUID
    index: int
    Premiums: float
    Acquisition_Comm : float
    Renewal_Comm : float
    Acq_Exp_Attr : float
    Maint_Exp_Attr : float
    Acq_Exp_N_Attr : float
    Maint_Exp_N_Attr : float
    Claims : float
    Total_Net_CFs : float
    Active_Flag : Optional[bool] = True
    Created_By : str
    Created_Date : datetime
    Modified_By : Optional[str] = None
    Modified_Date : Optional[datetime] = None
    
class CreateLiabilityInitRecRequest(BaseModel):
    Liability_Init_Rec_ID: UUID
    Run_ID: UUID
    index: int
    PV_Prem : float
    PV_RenComm : float
    PV_Claims : float
    PV_DirAqis : float
    PV_RiskAdj : float
    Total : float
    CSM_Init : float
    Liab_Init : float
    Active_Flag: Optional[bool] = True
    Created_By: str
    Created_Date: datetime
    Modified_By: Optional[str] = None
    Modified_Date: Optional[datetime] = None
    
class CreateRecBelRequest(BaseModel):
    Rec_Bec_ID: UUID
    Run_ID: UUID
    index: int
    Opening : float
    NewBusiness : float
    Assump : float
    ExpInFlow : float
    ExpOutFlow : float
    FinExp : float
    Changes_Exp : float
    Changes_Rel : float
    Closes : float
    Active_Flag: Optional[bool] = True
    Created_By: str
    Created_Date: datetime
    Modified_By: Optional[str] = None
    Modified_Date: Optional[datetime] = None
    
class LoginRequest(BaseModel):
    email: str
    password: str
