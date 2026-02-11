from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import logging
import os
import json
from typing import Optional, List, Dict, Any
import time
from threading import Thread, Lock
from datetime import time as dtime

# ---------------- Celery imports commented out ----------------
# from scheduler.celery_app import celery_app, update_schedule, load_schedules_from_db

from scheduler.scheduler import run_sbp_pipeline, run_secp_pipeline, run_sama_pipeline
from storage.mssql_repo import MSSQLRepository

logger = logging.getLogger(__name__)
app = FastAPI(title="Regulatory Pipeline API", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= DB SETUP =================
repo = MSSQLRepository({
    "server": os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver": os.getenv("MSSQL_DRIVER")
})

# Mapping regulator names to functions
REGULATOR_PIPELINES = {
    "SBP": run_sbp_pipeline,
    "SECP": run_secp_pipeline,
    "SAMA": run_sama_pipeline
}

pipeline_lock = Lock()

def update_heartbeat(regulator: str):
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE pipeline_status
            SET last_heartbeat = GETUTCDATE()
            WHERE regulator=? AND status='RUNNING'
        """, regulator)
        conn.commit()


# ================= HELPER FUNCTIONS =================
def serialize_datetime(obj):
    """Convert datetime objects to ISO format string"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def row_to_dict(row, columns):
    """Convert database row to dictionary with datetime serialization"""
    result = {}
    for col, value in zip(columns, row):
        result[col] = serialize_datetime(value)
    return result


# ---------- Background pipeline runner ----------
from threading import Thread

def run_pipeline_async(regulator: str):

    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_status (regulator, status, started_at, last_heartbeat)
            VALUES (?, 'RUNNING', GETUTCDATE(), GETUTCDATE())
        """, regulator)
        conn.commit()

    stop_heartbeat = False

    def heartbeat_loop():
        while not stop_heartbeat:
            update_heartbeat(regulator)
            time.sleep(300)  # every 5 minutes

    heartbeat_thread = Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    try:
        REGULATOR_PIPELINES[regulator]()

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pipeline_status
                SET status='DONE', finished_at=GETUTCDATE()
                WHERE regulator=? AND status='RUNNING'
            """, regulator)
            conn.commit()

    except Exception as e:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pipeline_status
                SET status='FAILED', finished_at=GETUTCDATE(), error=?
                WHERE regulator=? AND status='RUNNING'
            """, str(e), regulator)
            conn.commit()

    finally:
        stop_heartbeat = True

def scheduler_loop():
    logger.info("Scheduler started")

    while True:
        now = datetime.utcnow().time()

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 1 id, regulator
                FROM pipeline_schedule
                WHERE scheduled_time <= ?
                  AND status = 'PENDING'
                ORDER BY scheduled_time
            """, now)

            job = cursor.fetchone()

        if job:
            schedule_id, regulator = job

            if pipeline_lock.acquire(blocking=False):
                try:
                    with repo._get_conn() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE pipeline_schedule
                            SET status='RUNNING'
                            WHERE id=?
                        """, schedule_id)
                        conn.commit()

                    run_pipeline_async(regulator)

                    with repo._get_conn() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE pipeline_schedule
                            SET status='DONE',
                                last_run_at = GETUTCDATE()
                            WHERE id=?
                        """, schedule_id)
                        conn.commit()

                finally:
                    pipeline_lock.release()

        time.sleep(30)

def run_regulator_pipeline(regulator: str):
    try:
        logger.info(f"Starting {regulator} pipeline")
        pipeline_func = REGULATOR_PIPELINES.get(regulator)
        if pipeline_func:
            pipeline_func()
        logger.info(f"{regulator} pipeline completed")
    except Exception as e:
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)


# ---------- API Models ----------
class ScheduleUpdate(BaseModel):
    regulator: str
    hour: int
    minute: int


class RegulationResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    pagination: Dict[str, Any]


class ComplianceAnalysisResponse(BaseModel):
    success: bool
    data: Dict[str, Any]

class CategoryInfo(BaseModel):
    id: Optional[int]
    title: Optional[str]
    parent_id: Optional[int]

class RegulationModel(BaseModel):
    id: int
    regulator: str
    source_system: Optional[str]
    category: Optional[str]
    title: str
    document_url: Optional[str]
    document_html: Optional[str]
    published_date: Optional[str]
    reference_no: Optional[str]
    department: Optional[str]
    year: Optional[int]
    source_page_url: Optional[str]
    extra_meta: Optional[Dict[str, Any]]
    created_at: Optional[str]
    updated_at: Optional[str]
    category_info: Optional[CategoryInfo]

class SingleRegulationResponse(BaseModel):
    success: bool
    data: RegulationModel


# ---------- API Endpoints ----------
@app.on_event("startup")
def start_scheduler():
    Thread(target=scheduler_loop, daemon=True).start()

@app.post("/schedule")
def schedule_pipeline(regulator: str, hour: int, minute: int):

    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(400, "Unknown regulator")

    scheduled_time = dtime(hour, minute)

    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            MERGE pipeline_schedule AS target
            USING (SELECT ? AS regulator) AS src
            ON target.regulator = src.regulator
            WHEN MATCHED THEN
                UPDATE SET scheduled_time=?, status='PENDING'
            WHEN NOT MATCHED THEN
                INSERT (regulator, scheduled_time)
                VALUES (?, ?);
        """, regulator, scheduled_time, regulator, scheduled_time)
        conn.commit()

    return {
        "success": True,
        "regulator": regulator,
        "scheduled_time": f"{hour:02d}:{minute:02d}"
    }


@app.post("/update-schedule")
def update_pipeline_schedule(payload: ScheduleUpdate):
    if payload.regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")

    # ---------------- Celery schedule update commented out ----------------
    # update_schedule(payload.regulator, payload.hour, payload.minute)

    return {
        "status": "success",
        "regulator": payload.regulator,
        "hour": payload.hour,
        "minute": payload.minute,
        "message": f"{payload.regulator} schedule updated successfully"
    }
@app.post("/trigger/full")
def trigger_full_pipeline():
    completed_regulators = []
    errors = []

    for regulator, pipeline_func in REGULATOR_PIPELINES.items():
        try:
            logger.info(f"Starting {regulator} pipeline")
            pipeline_func()
            completed_regulators.append(regulator)
            logger.info(f"{regulator} pipeline completed")
        except Exception as e:
            logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
            errors.append({"regulator": regulator, "error": str(e)})

    return {
        "status": "done" if not errors else "partial_failure",
        "completed_regulators": completed_regulators,
        "errors": errors,
        "completed_at": datetime.utcnow().isoformat()
    }

@app.post("/trigger/{regulator}")
def trigger_regulator_pipeline(regulator: str):
    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")

    try:
        logger.info(f"Starting {regulator} pipeline (synchronous call)")
        pipeline_func = REGULATOR_PIPELINES[regulator]
        pipeline_func()  # <-- runs immediately without Celery
        logger.info(f"{regulator} pipeline completed")

        return {
            "status": "done",  # indicate the pipeline has completed
            "regulator": regulator,
            "completed_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"{regulator} pipeline failed: {e}")



# ================= NEW GET API: REGULATIONS BY REGULATOR =================
@app.get("/regulations/{regulator}", response_model=RegulationResponse)
def get_regulations_by_regulator(
        regulator: str,
        category_id: Optional[int] = Query(None, description="Filter by compliance category ID"),
        year: Optional[int] = Query(None, description="Filter by year"),
        limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
        offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get all regulations for a specific regulator with optional filters.
    """
    try:
        query = """
            SELECT 
                r.id,
                r.regulator,
                r.source_system,
                r.category,
                r.title,
                r.document_url,
                r.document_html,
                TRY_CONVERT(DATETIME, r.published_date, 103) AS published_date,
                r.reference_no,
                r.department,
                r.[year],
                r.source_page_url,
                r.extra_meta,
                TRY_CAST(r.created_at AS DATETIME) AS created_at,
                TRY_CAST(r.updated_at AS DATETIME) AS updated_at,
                r.compliancecategory_id,
                cc.title AS category_title,
                cc.parentid AS category_parent_id
            FROM regulations r
            LEFT JOIN compliancecategory cc 
                ON r.compliancecategory_id = cc.compliancecategory_id
            WHERE r.regulator = ?
        """

        params = [regulator.upper()]

        if category_id is not None:
            query += " AND r.compliancecategory_id = ?"
            params.append(category_id)

        if year is not None:
            query += " AND r.[year] = ?"
            params.append(year)

        # Pagination
        query += " ORDER BY r.published_date DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, limit])

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            regulations = []
            for row in rows:
                reg_dict = row_to_dict(row, columns)

                # Keep document_html but ignore extra_meta.org_pdf_html
                if reg_dict.get('extra_meta'):
                    try:
                        reg_dict['extra_meta'] = json.loads(reg_dict['extra_meta'])
                        # Remove org_pdf_html if present
                        reg_dict['extra_meta'].pop('org_pdf_html', None)
                    except:
                        pass

                # Add category info
                reg_dict['category_info'] = {
                    'id': reg_dict.pop('compliancecategory_id', None),
                    'title': reg_dict.pop('category_title', None),
                    'parent_id': reg_dict.pop('category_parent_id', None)
                }

                regulations.append(reg_dict)

            # Total count for pagination
            count_query = "SELECT COUNT(*) FROM regulations WHERE regulator = ?"
            count_params = [regulator.upper()]
            if category_id is not None:
                count_query += " AND compliancecategory_id = ?"
                count_params.append(category_id)
            if year is not None:
                count_query += " AND [year] = ?"
                count_params.append(year)

            cursor.execute(count_query, count_params)
            total_count = cursor.fetchone()[0]

        return {
            "success": True,
            "data": regulations,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + limit) < total_count,
                "current_page": (offset // limit) + 1,
                "total_pages": (total_count + limit - 1) // limit
            }
        }

    except Exception as e:
        logger.exception(f"Error fetching regulations for {regulator}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch regulations: {str(e)}")


# ================= GET Regulation BY REGULATION ID =================
@app.get("/regulation/{regulation_id}", response_model=SingleRegulationResponse)
def get_regulation_detail(regulation_id: int):
    """
    Get details for a specific regulation by its ID.
    """
    try:
        query = """
            SELECT 
                r.id,
                r.regulator,
                r.source_system,
                r.category,
                r.title,
                r.document_url,
                r.document_html,
                TRY_CONVERT(DATETIME, r.published_date, 103) AS published_date,
                r.reference_no,
                r.department,
                r.[year],
                r.source_page_url,
                r.extra_meta,
                TRY_CAST(r.created_at AS DATETIME) AS created_at,
                TRY_CAST(r.updated_at AS DATETIME) AS updated_at,
                r.compliancecategory_id,
                cc.title AS category_title,
                cc.parentid AS category_parent_id
            FROM regulations r
            LEFT JOIN compliancecategory cc 
                ON r.compliancecategory_id = cc.compliancecategory_id
            WHERE r.id = ?
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [regulation_id])
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Regulation not found")

            columns = [col[0] for col in cursor.description]
            reg_dict = row_to_dict(row, columns)

            # Parse extra_meta and remove org_pdf_html
            if reg_dict.get('extra_meta'):
                try:
                    reg_dict['extra_meta'] = json.loads(reg_dict['extra_meta'])
                    reg_dict['extra_meta'].pop('org_pdf_html', None)
                except:
                    pass

            # Add category info
            reg_dict['category_info'] = {
                'id': reg_dict.pop('compliancecategory_id', None),
                'title': reg_dict.pop('category_title', None),
                'parent_id': reg_dict.pop('category_parent_id', None)
            }

        return {
            "success": True,
            "data": reg_dict
        }

    except Exception as e:
        logger.exception(f"Error fetching regulation {regulation_id}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch regulation: {str(e)}")

# ================= GET COMPLIANCE ANALYSIS BY REGULATION ID =================
@app.get("/compliance-analysis/{regulation_id}", response_model=ComplianceAnalysisResponse)
def get_compliance_analysis(regulation_id: int):
    try:
        query = """
            SELECT
                id,
                regulation_id,
                analysis_json,
                TRY_CAST(created_at AS DATETIME) AS created_at,
                TRY_CAST(updated_at AS DATETIME) AS updated_at
            FROM compliance_analysis
            WHERE regulation_id = ?
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, regulation_id)
            row = cursor.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Compliance analysis not found for regulation ID {regulation_id}"
                )

            columns = [col[0] for col in cursor.description]
            analysis_dict = row_to_dict(row, columns)

            # Parse analysis_json if it is JSON
            if analysis_dict.get("analysis_json"):
                try:
                    analysis_dict["analysis_json"] = json.loads(analysis_dict["analysis_json"])
                except:
                    pass

            return {
                "success": True,
                "data": analysis_dict
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching compliance analysis for regulation {regulation_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch compliance analysis: {str(e)}"
        )


# ================= GET ALL COMPLIANCE CATEGORIES =================
@app.get("/categories")
def get_categories():
    """
    Get all compliance categories with hierarchy.

    **Returns:**
    - All categories and hierarchical structure
    """
    try:
        query = """
            SELECT 
                compliancecategory_id,
                title,
                parentid
            FROM compliancecategory
            ORDER BY parentid, title
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            categories = [row_to_dict(row, columns) for row in rows]

            # Organize into hierarchy
            categories_by_id = {cat['compliancecategory_id']: cat for cat in categories}
            root_categories = []

            for cat in categories:
                cat['children'] = []
                if cat['parentid'] is None:
                    root_categories.append(cat)
                else:
                    parent = categories_by_id.get(cat['parentid'])
                    if parent:
                        parent['children'].append(cat)

            return {
                'success': True,
                'data': {
                    'all_categories': categories,
                    'hierarchy': root_categories,
                    'total_count': len(categories)
                }
            }

    except Exception as e:
        logger.exception("Error fetching categories")
        raise HTTPException(status_code=500, detail=f"Failed to fetch categories: {str(e)}")

@app.get("/categories/roots")
def get_root_categories_only():
    try:
        query = """
            SELECT compliancecategory_id, title, parentid
            FROM compliancecategory
            WHERE parentid IS NULL
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            root_categories = [row_to_dict(row, columns) for row in rows]

        return {
            "success": True,
            "data": root_categories,
            "total": len(root_categories)
        }

    except Exception as e:
        logger.exception("Error fetching root categories")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/categories/root")
def get_root_categories_with_children():
    try:
        query = """
            SELECT compliancecategory_id, title, parentid
            FROM compliancecategory
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            categories = [row_to_dict(row, columns) for row in rows]

        # Index categories
        categories_by_id = {c["compliancecategory_id"]: c for c in categories}

        # Initialize children list
        for c in categories:
            c["children"] = []

        # Get root categories
        root_categories = [c for c in categories if c["parentid"] is None]

        # Attach ONLY direct children to root categories
        for root in root_categories:
            root_id = root["compliancecategory_id"]
            root["children"] = [
                c for c in categories if c["parentid"] == root_id
            ]

        return {
            "success": True,
            "data": root_categories,
            "total_root_categories": len(root_categories)
        }

    except Exception as e:
        logger.exception("Error fetching root categories")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/categories/children/{parent_id}")
def get_children(parent_id: int):
    try:
        query = """
            SELECT compliancecategory_id, title, parentid
            FROM compliancecategory
            WHERE parentid = ?
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, parent_id)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            children = [row_to_dict(row, columns) for row in rows]

        return {"success": True, "data": children}

    except Exception as e:
        raise HTTPException(500, str(e))



@app.get("/status/full")
def get_full_status():

    results = {}

    for regulator in REGULATOR_PIPELINES.keys():
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 1 status
                FROM pipeline_status
                WHERE regulator=?
                ORDER BY id DESC
            """, regulator)

            row = cursor.fetchone()
            results[regulator] = row[0] if row else "NOT_STARTED"

    return {
        "pipeline_status": results,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/status/{regulator}")
def get_regulator_status(regulator: str):

    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 1 status, started_at, finished_at, error
            FROM pipeline_status
            WHERE regulator=?
            ORDER BY id DESC
        """, regulator)

        row = cursor.fetchone()

    if not row:
        return {"regulator": regulator, "status": "NOT_STARTED"}

    return {
        "regulator": regulator,
        "status": row[0],
        "started_at": serialize_datetime(row[1]),
        "finished_at": serialize_datetime(row[2]),
        "error": row[3]
    }

# ================= HEALTH CHECK =================
@app.get("/health")
def health_check():
    """Health check endpoint to verify API and database connectivity"""
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()

        return {
            'success': True,
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.exception("Health check failed")
        return {
            'success': False,
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


# ================= ROOT ENDPOINT =================
@app.get("/")
def root():
    """API root endpoint with available endpoints"""
    return {
        "message": "Regulatory Pipeline API",
        "version": "2.0.0",
        "endpoints": {
            "pipelines": {
                "trigger_specific": "POST /trigger/{regulator}",
                "trigger_all": "POST /trigger/full",
                "update_schedule": "POST /update-schedule"
            },
            "data": {
                "regulations_by_regulator": "GET /regulations/{regulator}",
                "compliance_analysis": "GET /compliance-analysis/{regulation_id}",
                "categories": "GET /categories",
                "statistics": "GET /statistics"
            },
            "health": "GET /health"
        },
        "available_regulators": list(REGULATOR_PIPELINES.keys())
    }
