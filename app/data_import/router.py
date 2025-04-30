# /home/chris/pandas/app/data_import/router.py
import os
import shutil
import tempfile
import traceback
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from sqlmodel import Session
from app.core.database import get_session 
from app.services.import_service import ImportService
from app.services.order_sync_service import OrderSyncService

router = APIRouter(
    tags=["import"],
)

# Dependency function to get ImportService
def get_import_service(session: Session = Depends(get_session)) -> ImportService:
    try:
        # Assuming ImportService takes the session in its constructor
        # Modify if ImportService initialization is different
        return ImportService(session=session)
    except Exception as e:
        print(f"Error initializing ImportService: {e}")
        traceback.print_exc() # Log detailed error during initialization
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not initialize import service."
        )

# Dependency function to get OrderSyncService
def get_order_sync_service(session: Session = Depends(get_session)) -> OrderSyncService:
    try:
        # OrderSyncService can also accept a session
        return OrderSyncService(session=session)
    except Exception as e:
        print(f"Error initializing OrderSyncService: {e}")
        traceback.print_exc() 
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not initialize order sync service."
        )

@router.post("/upload_excel", summary="Upload, process, and sync Excel file")
async def upload_excel(
    file: UploadFile = File(..., description="Excel file (.xlsx, .xls) to import"),
    import_service: ImportService = Depends(get_import_service),
    order_sync_service: OrderSyncService = Depends(get_order_sync_service),
):
    """
    Receives an Excel file, saves it temporarily, uses the ImportService
    to upsert data into the original_orders table, and then triggers
    OrderSyncService to sync data to sample_orders and bulk_orders.
    """
    allowed_extensions = {'.xlsx', '.xls'}
    _, file_ext = os.path.splitext(file.filename)
    if file_ext.lower() not in allowed_extensions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file type: {file_ext}. Only {', '.join(allowed_extensions)} allowed.",
        )

    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file.filename)

    try:
        # Save the uploaded file
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        print(f"Saved uploaded file temporarily to: {temp_file_path}")

        # Step 1: Call the import service's method
        print(f"Processing file with ImportService: {temp_file_path}")
        import_result = import_service.upsert_excel_to_db(file_path=temp_file_path)
        print(f"Import service finished. Result: {import_result}")
        
        # Check if import was successful before syncing
        # Assuming import_result is a dict like {'inserted': x, 'updated': y, 'errors': z}
        if import_result.get("errors", 0) > 0:
             # If there were import errors, report them and don't sync
             error_message = f"Import completed with {import_result.get('errors', 0)} errors."
             print(error_message)
             # Consider returning success=False or a specific error response
             return {"success": False, "message": error_message, "details": import_result}
        
        print("Import successful. Proceeding with order synchronization...")
        
        # Step 2: Trigger the order sync service
        try:
            sync_result = order_sync_service.sync_all_orders()
            print(f"Order synchronization finished. Result: {sync_result}")
            
            # Check sync results for errors
            sync_errors = sync_result.get("sample_orders", {}).get("errors", 0) + \
                          sync_result.get("bulk_orders", {}).get("errors", 0)
            
            if sync_errors > 0:
                sync_message = f"Import succeeded, but synchronization completed with {sync_errors} errors."
                print(sync_message)
                # Return success=True because import worked, but include sync details
                return {"success": True, "message": sync_message, "import_details": import_result, "sync_details": sync_result}
            else:
                 # Update success message
                 final_message = f"File '{file.filename}' processed and orders synchronized successfully."
                 print(final_message)
                 return {"success": True, "message": final_message, "import_details": import_result, "sync_details": sync_result}

        except Exception as sync_exc:
             sync_error_message = f"Import succeeded, but failed during order synchronization: {sync_exc}"
             print(sync_error_message)
             traceback.print_exc()
             # Return success=True (import worked) but report sync failure
             return {"success": True, "message": sync_error_message, "import_details": import_result}

    except HTTPException as http_exc:
        # Re-raise HTTPExceptions directly
        raise http_exc
    except Exception as e:
        print(f"Error processing file {file.filename}: {e}")
        traceback.print_exc() # Log the full stack trace for debugging
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process file '{file.filename}'. Internal server error.",
            # Avoid exposing raw error details like str(e) in production
        )
    finally:
        # Clean up temporary file and directory
        if os.path.exists(temp_dir):
            print(f"Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
        # Close the uploaded file stream
        await file.close()
