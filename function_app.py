"""
CST8917 - Smart PDF Analyzer with Durable Functions
=====================================================

Architecture (Fan-Out/Fan-In + Chaining):

    [Blob Trigger]
          |
          v
    [Orchestrator] -- starts orchestration with PDF blob name/path
          |
          |--- Fan-Out (parallel) ---
          |       extract_text
          |       extract_metadata
          |       analyze_statistics
          |       detect_sensitive_data
          |--- Fan-In (wait for all 4) ---
          v
    [combine_report]  (Chaining step 1: build unified report)
          v
    [store_results]   (Chaining step 2: write to Table Storage)
          v
    [HTTP Endpoint] -- GET /api/results/{document_id} --> returns JSON report

Function ownership in this file (3-person team):
  - Person A (this file's author): blob_trigger, orchestrator, fan-out/fan-in
    wiring, logging
  - Person B: extract_text, extract_metadata  (TODO markers below)
  - Person C: analyze_statistics, detect_sensitive_data, combine_report,
    store_results (TODO markers below)
  - HTTP endpoint (get_results) is shared/owned by whoever finishes first;
    stub included so the pipeline is testable end-to-end today.
"""

import logging
import json
import uuid
import os
from datetime import datetime, timezone

import azure.functions as func
import azure.durable_functions as df

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "pdfs")
TABLE_NAME = os.environ.get("TABLE_NAME", "PdfAnalysisResults")


# ---------------------------------------------------------------------------
# 1) BLOB TRIGGER
# ---------------------------------------------------------------------------
# Fires automatically whenever a new PDF is uploaded to the "pdfs" container.
# Its only job is to start a new orchestration instance and pass along the
# blob's name and path. Keep this function thin -- all real work happens in
# the orchestrator and activities.
@app.blob_trigger(
    arg_name="myblob",
    path=f"{BLOB_CONTAINER_NAME}/{{name}}",
    connection="AzureWebJobsStorage",
)
@app.durable_client_input(client_name="client")
async def blob_trigger(myblob: func.InputStream, client):
    logging.info(
        f"[blob_trigger] Detected new blob: name={myblob.name}, "
        f"size={myblob.length} bytes"
    )

    # Guard: only process PDFs (defensive check in case the container
    # receives a non-PDF file).
    if not myblob.name.lower().endswith(".pdf"):
        logging.warning(
            f"[blob_trigger] Skipping non-PDF blob: {myblob.name}"
        )
        return

    # Build the input payload passed into the orchestrator.
    # blob_path is the path the activities will use to re-fetch the blob
    # via the blob SDK (container + blob name).
    blob_name_only = myblob.name.split("/", 1)[-1]  # strip container prefix
    document_id = str(uuid.uuid4())

    orchestration_input = {
        "document_id": document_id,
        "blob_name": blob_name_only,
        "container": BLOB_CONTAINER_NAME,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }

    instance_id = await client.start_new(
        "orchestrator", client_input=orchestration_input
    )

    logging.info(
        f"[blob_trigger] Started orchestration instance_id={instance_id} "
        f"for document_id={document_id} (blob={blob_name_only})"
    )


# ---------------------------------------------------------------------------
# 2) ORCHESTRATOR -- Fan-Out/Fan-In + Chaining
# ---------------------------------------------------------------------------
@app.orchestration_trigger(context_name="context")
def orchestrator(context: df.DurableOrchestrationContext):
    input_data = context.get_input()
    document_id = input_data["document_id"]
    blob_name = input_data["blob_name"]
    container = input_data["container"]

    # context.create_replay_safe_logger avoids duplicate log lines during
    # Durable Functions replay -- use this instead of plain `logging`
    # inside the orchestrator body.
    logger = logging.getLogger(__name__)

    logger.info(
        f"[orchestrator] Starting orchestration for document_id={document_id}, "
        f"blob={blob_name}"
    )

    activity_input = {
        "document_id": document_id,
        "blob_name": blob_name,
        "container": container,
    }

    # ---- FAN-OUT ----
    # Kick off all 4 analysis activities in parallel. None of these calls
    # block here -- they just schedule the tasks.
    logger.info(f"[orchestrator] Fanning out 4 parallel activities for {blob_name}")

    parallel_tasks = [
        context.call_activity("extract_text", activity_input),
        context.call_activity("extract_metadata", activity_input),
        context.call_activity("analyze_statistics", activity_input),
        context.call_activity("detect_sensitive_data", activity_input),
    ]

    # ---- FAN-IN ----
    # task_all suspends the orchestrator until ALL 4 activities complete.
    # Durable Functions handles this efficiently (no compute billed while
    # waiting), and replays deterministically on resume.
    results = yield context.task_all(parallel_tasks)

    text_result, metadata_result, stats_result, sensitive_result = results

    logger.info(
        f"[orchestrator] Fan-in complete for document_id={document_id}. "
        f"All 4 activities returned successfully."
    )

    # ---- CHAINING ----
    # Step 1: combine all 4 results into one unified report.
    combine_input = {
        "document_id": document_id,
        "blob_name": blob_name,
        "uploaded_at": input_data["uploaded_at"],
        "extract_text": text_result,
        "extract_metadata": metadata_result,
        "analyze_statistics": stats_result,
        "detect_sensitive_data": sensitive_result,
    }
    report = yield context.call_activity("combine_report", combine_input)
    logger.info(f"[orchestrator] Report combined for document_id={document_id}")

    # Step 2: persist the combined report to Table Storage.
    store_result = yield context.call_activity("store_results", report)
    logger.info(
        f"[orchestrator] Results stored for document_id={document_id}: "
        f"{store_result}"
    )

    return report


# ---------------------------------------------------------------------------
# 3-6) FOUR PARALLEL ACTIVITIES (fan-out targets)
# ---------------------------------------------------------------------------
# NOTE TO TEAM: These are STUBS so the orchestrator + blob trigger can be
# tested end-to-end right now. Replace the body of each function with real
# logic, but KEEP the function name, the @app.activity_trigger decorator,
# and the input/return shape the same so the orchestrator above doesn't
# need to change.
#
# Each activity receives `activity_input` containing:
#   { "document_id": str, "blob_name": str, "container": str }
#
# Use the blob SDK to fetch the actual PDF bytes, e.g.:
#
#   from azure.storage.blob import BlobServiceClient
#   conn_str = os.environ["AzureWebJobsStorage"]
#   blob_service = BlobServiceClient.from_connection_string(conn_str)
#   blob_client = blob_service.get_blob_client(
#       container=activity_input["container"], blob=activity_input["blob_name"]
#   )
#   pdf_bytes = blob_client.download_blob().readall()
#
# Then parse with pypdf / PyPDF2:
#
#   from pypdf import PdfReader
#   import io
#   reader = PdfReader(io.BytesIO(pdf_bytes))


@app.activity_trigger(input_name="activity_input")
def extract_text(activity_input: dict) -> dict:
    """TODO (Person B): Extract text content from all pages of the PDF."""
    logging.info(
        f"[extract_text] STARTED for {activity_input['blob_name']}"
    )

    # --- TODO: replace stub below with real pypdf extraction ---
    extracted_text = (
        f"[STUB] Extracted text would appear here for "
        f"{activity_input['blob_name']}"
    )
    page_texts = ["[STUB] page 1 text", "[STUB] page 2 text"]
    # --- end stub ---

    logging.info(
        f"[extract_text] FINISHED for {activity_input['blob_name']}"
    )
    return {
        "full_text": extracted_text,
        "page_texts": page_texts,
    }


@app.activity_trigger(input_name="activity_input")
def extract_metadata(activity_input: dict) -> dict:
    """TODO (Person B): Extract PDF metadata (author, title, created date, etc.)."""
    logging.info(
        f"[extract_metadata] STARTED for {activity_input['blob_name']}"
    )

    # --- TODO: replace stub below with reader.metadata from pypdf ---
    metadata = {
        "title": "[STUB] Untitled",
        "author": "[STUB] Unknown",
        "creation_date": None,
        "producer": "[STUB] Unknown",
    }
    # --- end stub ---

    logging.info(
        f"[extract_metadata] FINISHED for {activity_input['blob_name']}"
    )
    return metadata


@app.activity_trigger(input_name="activity_input")
def analyze_statistics(activity_input: dict) -> dict:
    """TODO (Person C): Page count, word count, avg words/page, est. reading time."""
    logging.info(
        f"[analyze_statistics] STARTED for {activity_input['blob_name']}"
    )

    # --- TODO: compute real stats from extracted page text ---
    page_count = 2
    word_count = 0
    avg_words_per_page = 0
    estimated_reading_time_minutes = round(word_count / 200, 2)  # ~200 wpm
    # --- end stub ---

    logging.info(
        f"[analyze_statistics] FINISHED for {activity_input['blob_name']}"
    )
    return {
        "page_count": page_count,
        "word_count": word_count,
        "avg_words_per_page": avg_words_per_page,
        "estimated_reading_time_minutes": estimated_reading_time_minutes,
    }


@app.activity_trigger(input_name="activity_input")
def detect_sensitive_data(activity_input: dict) -> dict:
    """TODO (Person C): Regex-scan for emails, phone numbers, URLs, dates."""
    logging.info(
        f"[detect_sensitive_data] STARTED for {activity_input['blob_name']}"
    )

    # --- TODO: run regex patterns against extracted text ---
    findings = {
        "emails": [],
        "phone_numbers": [],
        "urls": [],
        "dates": [],
    }
    # --- end stub ---

    logging.info(
        f"[detect_sensitive_data] FINISHED for {activity_input['blob_name']}"
    )
    return findings


# ---------------------------------------------------------------------------
# 7) COMBINE_REPORT -- Chaining step 1
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="combine_input")
def combine_report(combine_input: dict) -> dict:
    """TODO (Person C): Merge all 4 activity results into one unified report.

    Stub below already does a reasonable merge -- team can extend with
    extra derived fields (e.g. a risk flag if sensitive data was found).
    """
    logging.info(
        f"[combine_report] Building unified report for "
        f"document_id={combine_input['document_id']}"
    )

    report = {
        "document_id": combine_input["document_id"],
        "blob_name": combine_input["blob_name"],
        "uploaded_at": combine_input["uploaded_at"],
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "text_extraction": combine_input["extract_text"],
        "metadata": combine_input["extract_metadata"],
        "statistics": combine_input["analyze_statistics"],
        "sensitive_data": combine_input["detect_sensitive_data"],
    }

    logging.info(
        f"[combine_report] Report ready for document_id={combine_input['document_id']}"
    )
    return report


# ---------------------------------------------------------------------------
# 8) STORE_RESULTS -- Chaining step 2 (Azure Table Storage)
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="report")
def store_results(report: dict) -> dict:
    """TODO (Person C): Persist the combined report to Azure Table Storage.

    Stub below shows the real Table Storage write pattern -- fill in the
    connection string env var name to match whatever the team settles on.
    """
    logging.info(
        f"[store_results] Storing report for document_id={report['document_id']}"
    )

    try:
        from azure.data.tables import TableServiceClient

        conn_str = os.environ.get(
            "STORAGE_CONNECTION_STRING",
            os.environ.get("AzureWebJobsStorage", "UseDevelopmentStorage=true"),
        )
        table_service = TableServiceClient.from_connection_string(conn_str)
        table_client = table_service.create_table_if_not_exists(TABLE_NAME)

        entity = {
            "PartitionKey": "pdf-reports",
            "RowKey": report["document_id"],
            "blob_name": report["blob_name"],
            "uploaded_at": report["uploaded_at"],
            "processed_at": report["processed_at"],
            # Table Storage doesn't support nested objects, so complex
            # fields are serialized to JSON strings.
            "text_extraction": json.dumps(report["text_extraction"]),
            "metadata": json.dumps(report["metadata"]),
            "statistics": json.dumps(report["statistics"]),
            "sensitive_data": json.dumps(report["sensitive_data"]),
        }

        table_client.upsert_entity(entity)
        logging.info(
            f"[store_results] Stored document_id={report['document_id']} "
            f"in table '{TABLE_NAME}'"
        )
        return {"status": "stored", "document_id": report["document_id"]}

    except Exception as e:
        logging.error(
            f"[store_results] FAILED to store document_id="
            f"{report['document_id']}: {e}"
        )
        raise


# ---------------------------------------------------------------------------
# 9) HTTP ENDPOINT -- retrieve results by document_id
# ---------------------------------------------------------------------------
@app.route(route="results/{document_id}", methods=["GET"])
def get_results(req: func.HttpRequest) -> func.HttpResponse:
    """GET /api/results/{document_id} -> returns the stored JSON report."""
    document_id = req.route_params.get("document_id")
    logging.info(f"[get_results] Lookup requested for document_id={document_id}")

    if not document_id:
        return func.HttpResponse(
            json.dumps({"error": "document_id is required"}),
            status_code=400,
            mimetype="application/json",
        )

    try:
        from azure.data.tables import TableServiceClient
        from azure.core.exceptions import ResourceNotFoundError

        conn_str = os.environ.get(
            "STORAGE_CONNECTION_STRING",
            os.environ.get("AzureWebJobsStorage", "UseDevelopmentStorage=true"),
        )
        table_service = TableServiceClient.from_connection_string(conn_str)
        table_client = table_service.get_table_client(TABLE_NAME)

        entity = table_client.get_entity(
            partition_key="pdf-reports", row_key=document_id
        )

        # Deserialize the JSON-string fields back into objects for the
        # HTTP response.
        result = {
            "document_id": entity["RowKey"],
            "blob_name": entity.get("blob_name"),
            "uploaded_at": entity.get("uploaded_at"),
            "processed_at": entity.get("processed_at"),
            "text_extraction": json.loads(entity.get("text_extraction", "{}")),
            "metadata": json.loads(entity.get("metadata", "{}")),
            "statistics": json.loads(entity.get("statistics", "{}")),
            "sensitive_data": json.loads(entity.get("sensitive_data", "{}")),
        }

        logging.info(f"[get_results] Found result for document_id={document_id}")
        return func.HttpResponse(
            json.dumps(result, indent=2),
            status_code=200,
            mimetype="application/json",
        )

    except ResourceNotFoundError:
        logging.warning(f"[get_results] No result found for document_id={document_id}")
        return func.HttpResponse(
            json.dumps({"error": f"No results found for document_id={document_id}"}),
            status_code=404,
            mimetype="application/json",
        )
    except Exception as e:
        logging.error(f"[get_results] ERROR for document_id={document_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )
