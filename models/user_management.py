import uuid

from sqlalchemy import Column, String, Boolean, Text, ForeignKey, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from models.base import Base

class User(Base):
    __tablename__ = "User"
    __table_args__ = {"schema": "UserManagement"}

    User_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    User_Name = Column(String(100), nullable=False)
    User_Group = Column(String(100), ForeignKey("UserManagement.User_Group.User_Group_Name"), nullable=False)
    Email_ID = Column(String(100), nullable=True)
    User_Desc = Column(Text, nullable=True)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)

    # Relationship with UserGroup
    user_group = relationship("UserGroup", back_populates="users")

    # Relationship with Run
    runs = relationship("Run", back_populates="user")
    
    run_inputs = relationship("RunInput", back_populates="user")
    
    coverage_units_rec = relationship("CoverageUnitsRec", back_populates="user")
    
    cash_flow = relationship("CashFlow", back_populates="user")
    
    liability_init_rec = relationship("Liability_Init_Rec", back_populates="user")

    rec_bel_updated = relationship("Rec_Bel_Updated", back_populates="user")

class UserGroup(Base):
    __tablename__ = "User_Group"
    __table_args__ = {"schema": "UserManagement"}

    User_Group_ID = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    User_Group_Name = Column(String(100), nullable=False)
    User_Group_Desc = Column(Text, nullable=True)
    Active_Flag = Column(Boolean, default=True, nullable=False)
    Created_By = Column(String(100), nullable=False)
    Created_Date = Column(TIMESTAMP(timezone=True), nullable=False)
    Modified_By = Column(String(100), nullable=True)
    Modified_Date = Column(TIMESTAMP(timezone=True), nullable=True)

    # Relationship with User
    users = relationship("User", back_populates="user_group")
