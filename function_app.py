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

Function ownership:
  - Member 1: blob_trigger, orchestrator, combine_report, store_results, get_results
  - Member 2: extract_text, extract_metadata, analyze_statistics, detect_sensitive_data
"""

import io
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone

import azure.functions as func
import azure.durable_functions as df
import pypdf
from azure.storage.blob import BlobServiceClient

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "pdfs")
TABLE_NAME = os.environ.get("TABLE_NAME", "PdfAnalysisResults")


# ---------------------------------------------------------------------------
# 1) BLOB TRIGGER
# ---------------------------------------------------------------------------
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

    if not myblob.name.lower().endswith(".pdf"):
        logging.warning(
            f"[blob_trigger] Skipping non-PDF blob: {myblob.name}"
        )
        return

    blob_name_only = myblob.name.split("/", 1)[-1]
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
    logger.info(f"[orchestrator] Fanning out 4 parallel activities for {blob_name}")

    parallel_tasks = [
        context.call_activity("extract_text", activity_input),
        context.call_activity("extract_metadata", activity_input),
        context.call_activity("analyze_statistics", activity_input),
        context.call_activity("detect_sensitive_data", activity_input),
    ]

    # ---- FAN-IN ----
    results = yield context.task_all(parallel_tasks)

    text_result, metadata_result, stats_result, sensitive_result = results

    logger.info(
        f"[orchestrator] Fan-in complete for document_id={document_id}. "
        f"All 4 activities returned successfully."
    )

    # ---- CHAINING ----
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

    store_result = yield context.call_activity("store_results", report)
    logger.info(
        f"[orchestrator] Results stored for document_id={document_id}: "
        f"{store_result}"
    )

    return report


# ---------------------------------------------------------------------------
# MEMBER 2: PDF Analysis Engine
# ---------------------------------------------------------------------------
# Shared helper -- downloads the blob and returns a PdfReader.
# All four activity functions below call this instead of duplicating
# the blob client setup.

def _read_pdf(blob_name: str, container: str) -> pypdf.PdfReader:
    """Download a PDF blob from Azure Storage and return a PdfReader."""
    blob_service = BlobServiceClient.from_connection_string(
        os.environ["AzureWebJobsStorage"]
    )
    blob_client = blob_service.get_blob_client(
        container=container, blob=blob_name
    )
    pdf_bytes = blob_client.download_blob().readall()
    return pypdf.PdfReader(io.BytesIO(pdf_bytes))


# ---------------------------------------------------------------------------
# 3) extract_text
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="activity_input")
def extract_text(activity_input: dict) -> dict:
    """
    Extract readable text from every page of the uploaded PDF.

    Returns:
        {
            "full_text":  str,         # all pages joined
            "page_texts": [str],       # one entry per page
            "page_count": int
        }
    """
    blob_name = activity_input["blob_name"]
    container = activity_input["container"]

    logging.info("[extract_text] Starting for: %s", blob_name)

    try:
        reader     = _read_pdf(blob_name, container)
        page_texts = []
        parts      = []

        for i, page in enumerate(reader.pages):
            try:
                text = page.extract_text() or ""
            except Exception as e:
                logging.warning("[extract_text] Page %d error: %s", i + 1, e)
                text = ""
            page_texts.append(text.strip())
            parts.append(text)

        full_text = "\n".join(parts).strip()

        logging.info("[extract_text] Done – %d pages for %s", len(page_texts), blob_name)
        return {
            "full_text":  full_text,
            "page_texts": page_texts,
            "page_count": len(page_texts),
        }

    except Exception as e:
        logging.error("[extract_text] Failed for %s: %s", blob_name, e)
        return {"full_text": "", "page_texts": [], "page_count": 0, "error": str(e)}


# ---------------------------------------------------------------------------
# 4) extract_metadata
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="activity_input")
def extract_metadata(activity_input: dict) -> dict:
    """
    Extract PDF document metadata: title, author, creator, producer,
    subject, creation date, and modification date.

    Returns:
        {
            "title":             str | None,
            "author":            str | None,
            "creator":           str | None,
            "producer":          str | None,
            "subject":           str | None,
            "creation_date":     str | None,
            "modification_date": str | None
        }
    """
    blob_name = activity_input["blob_name"]
    container = activity_input["container"]

    logging.info("[extract_metadata] Starting for: %s", blob_name)

    def _clean(value) -> str | None:
        if value is None:
            return None
        s = str(value).strip()
        return s if s else None

    try:
        reader = _read_pdf(blob_name, container)
        meta   = reader.metadata or {}

        result = {
            "title":             _clean(meta.get("/Title")),
            "author":            _clean(meta.get("/Author")),
            "creator":           _clean(meta.get("/Creator")),
            "producer":          _clean(meta.get("/Producer")),
            "subject":           _clean(meta.get("/Subject")),
            "creation_date":     _clean(meta.get("/CreationDate")),
            "modification_date": _clean(meta.get("/ModDate")),
        }

        logging.info(
            "[extract_metadata] Done – title=%s, author=%s for %s",
            result["title"], result["author"], blob_name
        )
        return result

    except Exception as e:
        logging.error("[extract_metadata] Failed for %s: %s", blob_name, e)
        return {
            "title": None, "author": None, "creator": None,
            "producer": None, "subject": None,
            "creation_date": None, "modification_date": None,
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# 5) analyze_statistics
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="activity_input")
def analyze_statistics(activity_input: dict) -> dict:
    """
    Calculate page count, word count, average words per page,
    and estimated reading time (at 238 words per minute).

    Returns:
        {
            "page_count":                    int,
            "word_count":                    int,
            "avg_words_per_page":            float,
            "estimated_reading_time_minutes": float
        }
    """
    WORDS_PER_MINUTE = 238

    blob_name = activity_input["blob_name"]
    container = activity_input["container"]

    logging.info("[analyze_statistics] Starting for: %s", blob_name)

    try:
        reader     = _read_pdf(blob_name, container)
        page_count = len(reader.pages)
        parts      = []

        for i, page in enumerate(reader.pages):
            try:
                parts.append(page.extract_text() or "")
            except Exception as e:
                logging.warning("[analyze_statistics] Page %d error: %s", i + 1, e)

        words      = [w for w in " ".join(parts).split() if w.strip()]
        word_count = len(words)

        result = {
            "page_count":                     page_count,
            "word_count":                     word_count,
            "avg_words_per_page":             round(word_count / page_count, 2) if page_count else 0.0,
            "estimated_reading_time_minutes": round(word_count / WORDS_PER_MINUTE, 2),
        }

        logging.info(
            "[analyze_statistics] Done – %d pages, %d words for %s",
            page_count, word_count, blob_name
        )
        return result

    except Exception as e:
        logging.error("[analyze_statistics] Failed for %s: %s", blob_name, e)
        return {
            "page_count": 0, "word_count": 0,
            "avg_words_per_page": 0.0,
            "estimated_reading_time_minutes": 0.0,
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# 6) detect_sensitive_data
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="activity_input")
def detect_sensitive_data(activity_input: dict) -> dict:
    """
    Scan PDF text for sensitive patterns: emails, phone numbers,
    URLs, and dates. All matches are deduplicated.

    Returns:
        {
            "emails":        [str],
            "phone_numbers": [str],
            "urls":          [str],
            "dates":         [str],
            "summary": {
                "email_count": int, "phone_count": int,
                "url_count":   int, "date_count":  int,
                "total_findings": int
            }
        }
    """
    EMAIL_PATTERN = r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b'
    PHONE_PATTERN = r'(?<!\d)(?:\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}(?!\d)'
    URL_PATTERN   = r'\bhttps?://[^\s<>"\')\]]+(?<![.,;:!?])|www\.[^\s<>"\')\]]+(?<![.,;:!?])'
    DATE_PATTERN  = (
        r'\b(?:'
        r'\d{4}[-/]\d{2}[-/]\d{2}'
        r'|\d{2}[-/]\d{2}[-/]\d{4}'
        r'|(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?'
        r'|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?'
        r'|Dec(?:ember)?)\.?\s+\d{1,2},?\s+\d{4}'
        r'|\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May'
        r'|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?'
        r'|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{4}'
        r')\b'
    )

    blob_name = activity_input["blob_name"]
    container = activity_input["container"]

    logging.info("[detect_sensitive_data] Starting for: %s", blob_name)

    try:
        reader = _read_pdf(blob_name, container)
        parts  = []

        for i, page in enumerate(reader.pages):
            try:
                parts.append(page.extract_text() or "")
            except Exception as e:
                logging.warning("[detect_sensitive_data] Page %d error: %s", i + 1, e)

        full_text = "\n".join(parts)

        emails        = list(dict.fromkeys(re.findall(EMAIL_PATTERN, full_text, re.IGNORECASE)))
        phone_numbers = list(dict.fromkeys(re.findall(PHONE_PATTERN, full_text)))
        urls          = list(dict.fromkeys(re.findall(URL_PATTERN,   full_text)))
        dates         = list(dict.fromkeys(re.findall(DATE_PATTERN,  full_text, re.IGNORECASE)))

        summary = {
            "email_count":    len(emails),
            "phone_count":    len(phone_numbers),
            "url_count":      len(urls),
            "date_count":     len(dates),
            "total_findings": len(emails) + len(phone_numbers) + len(urls) + len(dates),
        }

        logging.info(
            "[detect_sensitive_data] Done – %d emails, %d phones, %d URLs, %d dates for %s",
            summary["email_count"], summary["phone_count"],
            summary["url_count"], summary["date_count"], blob_name,
        )

        return {
            "emails":        emails,
            "phone_numbers": phone_numbers,
            "urls":          urls,
            "dates":         dates,
            "summary":       summary,
        }

    except Exception as e:
        logging.error("[detect_sensitive_data] Failed for %s: %s", blob_name, e)
        return {
            "emails": [], "phone_numbers": [], "urls": [], "dates": [],
            "summary": {
                "email_count": 0, "phone_count": 0,
                "url_count": 0, "date_count": 0, "total_findings": 0,
            },
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# 7) COMBINE_REPORT -- Chaining step 1
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="combine_input")
def combine_report(combine_input: dict) -> dict:
    """Merge all 4 activity results into one unified report."""
    logging.info(
        f"[combine_report] Building unified report for "
        f"document_id={combine_input['document_id']}"
    )

    report = {
        "document_id":  combine_input["document_id"],
        "blob_name":    combine_input["blob_name"],
        "uploaded_at":  combine_input["uploaded_at"],
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "text_extraction": combine_input["extract_text"],
        "metadata":        combine_input["extract_metadata"],
        "statistics":      combine_input["analyze_statistics"],
        "sensitive_data":  combine_input["detect_sensitive_data"],
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
    """Persist the combined report to Azure Table Storage."""
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
        table_client  = table_service.create_table_if_not_exists(TABLE_NAME)

        entity = {
            "PartitionKey": "pdf-reports",
            "RowKey":       report["document_id"],
            "blob_name":    report["blob_name"],
            "uploaded_at":  report["uploaded_at"],
            "processed_at": report["processed_at"],
            "text_extraction": json.dumps(report["text_extraction"]),
            "metadata":        json.dumps(report["metadata"]),
            "statistics":      json.dumps(report["statistics"]),
            "sensitive_data":  json.dumps(report["sensitive_data"]),
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
        table_client  = table_service.get_table_client(TABLE_NAME)

        entity = table_client.get_entity(
            partition_key="pdf-reports", row_key=document_id
        )

        result = {
            "document_id": entity["RowKey"],
            "blob_name":   entity.get("blob_name"),
            "uploaded_at": entity.get("uploaded_at"),
            "processed_at": entity.get("processed_at"),
            "text_extraction": json.loads(entity.get("text_extraction", "{}")),
            "metadata":        json.loads(entity.get("metadata", "{}")),
            "statistics":      json.loads(entity.get("statistics", "{}")),
            "sensitive_data":  json.loads(entity.get("sensitive_data", "{}")),
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