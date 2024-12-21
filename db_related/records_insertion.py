import requests
from datetime import datetime

def send_bulk_records(dataframe, api_url):
    """
    Sends all records from the DataFrame to the specified API endpoint in bulk.
    
    Args:
        dataframe (pd.DataFrame): DataFrame containing the records to be sent
        api_url (str): URL of the API endpoint to send the records to (e.g. "http://localhost:8000/api/run") 
        
    Returns:
        None    
    """

    records = dataframe.to_dict(orient="records")
    
    for record in records:
        
        if 'Reporting_Date' in record:
            if isinstance(record['Reporting_Date'], datetime):
                record['Reporting_Date'] = record['Reporting_Date'].date().isoformat() 
            
        if 'Created_Date' in record:
            if isinstance(record['Created_Date'], datetime):
                record['Created_Date'] = record['Created_Date'].isoformat()
        
        if 'Modified_Date':
            if isinstance(record['Modified_Date'], datetime):
                record['Modified_Date'] = record['Modified_Date'].isoformat()
    
    payload = records   

    try:
        print(f"Sending {payload} records to {api_url}...")
        response = requests.post(api_url, json=payload)
        
        if response.status_code == 201:
            print("Records successfully sent!", response.json())
        else:
            print(f"Failed to send records: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Error occurred: {e}")