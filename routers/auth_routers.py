from schemas.db_schemas import LoginRequest
from fastapi import APIRouter, HTTPException

router = APIRouter()

# Define the login route
@router.post("/login")
async def login(request: LoginRequest):
    """
    API endpoint to login a user.

    Args:
    request : LoginRequest : The request object containing the email and password.

    Returns:
    dict : A dictionary containing the status of the login and a message.
    """
    # Validate credentials
    if request.email == "info@infigossoftwaresolutions.com" and request.password == "infigoss@1234":
        return {"status": "approved", "message": "Login successful"}
    else:
        # Raise 401 Unauthorized for incorrect credentials
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
