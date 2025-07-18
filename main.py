import os
import gspread
from google.oauth2.service_account import Credentials
import functions_framework
import json

# ==================== CONFIGURATION (FOR CLOUD FUNCTION ENVIRONMENT) ====================
# SPREADSHEET_ID will be loaded from Cloud Function environment variables
# GCP_SERVICE_ACCOUNT_KEY_JSON will be loaded from Cloud Function environment variables

# Defines the permissions the script needs
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Initialize gspread client and sheet globally to reuse connections across invocations
sheet_client = None
sheet = None

def get_sheet_client():
    """Initializes and returns the gspread client using service account JSON from environment."""
    global sheet_client
    if sheet_client is None:
        try:
            # Load service account key JSON from environment variable
            gcp_service_account_key_json = os.environ.get("GCP_SERVICE_ACCOUNT_KEY_JSON")
            if not gcp_service_account_key_json:
                raise ValueError("GCP_SERVICE_ACCOUNT_KEY_JSON environment variable not set.")

            creds_info = json.loads(gcp_service_account_key_json)
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
            sheet_client = gspread.authorize(creds)
            print("✅ Successfully connected to Google Sheet client.")
        except Exception as e:
            print(f"❌ Error initializing gspread client: {e}")
            raise
    return sheet_client

def get_sheet():
    """Opens the specific sheet and returns the first worksheet."""
    global sheet
    if sheet is None:
        try:
            spreadsheet_id = os.environ.get("SPREADSHEET_ID")
            if not spreadsheet_id:
                raise ValueError("SPREADSHEET_ID environment variable not set.")
            client = get_sheet_client()
            sheet = client.open_by_key(spreadsheet_id).sheet1
            print(f"✅ Successfully opened Google Sheet with ID: {spreadsheet_id}")
        except Exception as e:
            print(f"❌ Error opening Google Sheet: {e}")
            raise
    return sheet

@functions_framework.http
def update_google_sheet(request):
    """
    HTTP Cloud Function to update Google Sheet based on Twilio Studio webhook.
    This function expects a POST request with a JSON payload containing
    the sheetRowIndex and the playbook output parameters.

    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    # Ensure the request method is POST
    if request.method != 'POST':
        print(f"Method Not Allowed: Received {request.method} request.")
        return 'Method Not Allowed', 405

    # Parse the JSON payload from the request body
    request_json = request.get_json(silent=True)
    if not request_json:
        print("Bad Request: JSON payload missing or invalid.")
        return 'Bad Request: JSON payload missing or invalid', 400

    print(f"Received payload: {json.dumps(request_json, indent=2)}")

    # Extract parameters from the incoming webhook payload
    # These keys should match what you send from Twilio Studio
    sheet_row_index = request_json.get('sheetRowIndex')
    appointment_date = request_json.get('appointmentDate')
    appointment_time = request_json.get('appointmentTime')
    call_status = request_json.get('callStatus')
    call_summary = request_json.get('callSummary')
    # appointment_datetime = request_json.get('appointmentDatetime') # Optional, for logging/debugging

    # Validate sheet_row_index
    if not sheet_row_index:
        print("Bad Request: sheetRowIndex is required in the payload.")
        return 'Bad Request: sheetRowIndex is required', 400

    try:
        sheet_row_index = int(sheet_row_index)
    except ValueError:
        print(f"Bad Request: Invalid sheetRowIndex '{sheet_row_index}'. Must be an integer.")
        return 'Bad Request: sheetRowIndex must be an integer', 400

    try:
        worksheet = get_sheet()

        # Define column mappings for your Google Sheet (1-based indexing)
        # These correspond to your sheet:
        # Phone Number (1), Name (2), Business Type (3), Details (4),
        # Appointment Booked (5), Date of appointment (6), Call Summary (7),
        # Call Status (8), appointment time(starting) (9)
        COL_APPOINTMENT_BOOKED = 5  # Column E
        COL_DATE_OF_APPOINTMENT = 6 # Column F
        COL_CALL_SUMMARY = 7        # Column G
        COL_CALL_STATUS = 8         # Column H
        COL_APPOINTMENT_TIME = 9    # Column I

        updates = {}
        # Populate updates dictionary only if data is present
        if appointment_date is not None:
            updates[COL_DATE_OF_APPOINTMENT] = appointment_date
        if appointment_time is not None:
            updates[COL_APPOINTMENT_TIME] = appointment_time
        if call_summary is not None:
            updates[COL_CALL_SUMMARY] = call_summary
        if call_status is not None:
            updates[COL_CALL_STATUS] = call_status
            # Determine "Appointment Booked" status based on call_status
            if call_status.lower() == 'scheduled':
                updates[COL_APPOINTMENT_BOOKED] = 'Yes'
            elif call_status.lower() in ['declined', 'not interested', 'cancelled', 'no']:
                updates[COL_APPOINTMENT_BOOKED] = 'No'
            else:
                # If status is something else (e.g., 'callback', 'rescheduled'),
                # you might want to leave this blank or set to 'Pending'
                updates[COL_APPOINTMENT_BOOKED] = ''

        if not updates:
            print(f"No valid update parameters provided in the payload for row {sheet_row_index}.")
            return 'No valid updates provided', 200 # Still return 200 if nothing to update

        # Perform updates for each column
        for col, value in updates.items():
            # gspread's update_cell is efficient for single cell updates
            worksheet.update_cell(sheet_row_index, col, value)
            print(f"Updated row {sheet_row_index}, column {col} with value: '{value}'")

        print(f"Successfully processed and updated Google Sheet for row {sheet_row_index}.")
        return 'Google Sheet updated successfully', 200

    except Exception as e:
        print(f"❌ Error updating Google Sheet for row {sheet_row_index}: {e}")
        return f'Internal Server Error: {e}', 500
