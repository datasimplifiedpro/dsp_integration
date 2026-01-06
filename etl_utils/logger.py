import datetime
import traceback
import pandas as pd
import json
from sqlalchemy import text
from db_utils import get_mysql_engine
from app_config import DB_CONFIG

# Format: 'mysql+pymysql://<user>:<password>@<host>:<port>/<dbname>'
engine = get_mysql_engine(**DB_CONFIG)

class ETLLogger:
    def __init__(self, job_name):
        self.engine = engine
        self.job_name = job_name
        self.run_id = None
        self.start_time = datetime.datetime.utcnow()

    # def start(self, parameters=None):
    #     log = {
    #         'job_name': self.job_name,
    #         'start_time': self.start_time,
    #         'status': 'running',
    #         'run_parameters': str(parameters) if parameters else None
    #     }
    #     df = pd.DataFrame([log])
    #     df.to_sql('etl_job_log', self.engine, if_exists='append', index=False)
    #
    #     with self.engine.connect() as conn:
    #         result = conn.execute("SELECT LAST_INSERT_ID()").fetchone()
    #         self.run_id = result[0]
    #
    #     return self.run_id, self.start_time

    def start(self, parameters=None):
        run_parameters = json.dumps(parameters, default=str) if parameters else None

        insert_query = text("""
            INSERT INTO etl_job_log (job_name, start_time, status, run_parameters)
            VALUES (:job_name, :start_time, :status, :run_parameters)
        """)

        with self.engine.begin() as conn:
            conn.execute(insert_query, {
                "job_name": self.job_name,
                "start_time": self.start_time,
                "status": "running",
                "run_parameters": run_parameters
            })

            # Get last inserted ID
            result = conn.execute(text("SELECT LAST_INSERT_ID()")).fetchone()
            self.run_id = result[0]

        return self.run_id, self.start_time

    # def end(self, record_count=None, status='success', extra_info=None):
    #     end_time = datetime.datetime.utcnow()
    #     duration = (end_time - self.start_time).total_seconds()
    #     update_query = """
    #         UPDATE etl_job_log
    #         SET end_time = %s,
    #             duration_seconds = %s,
    #             record_count = %s,
    #             status = %s,
    #             extra_info = %s
    #         WHERE id = %s
    #     """
    #     values = (
    #         end_time,
    #         int(duration),
    #         record_count,
    #         status,
    #         str(extra_info) if extra_info else None,
    #         self.run_id
    #     )
    #     with self.engine.begin() as conn:
    #         conn.execute(update_query, values)

    from sqlalchemy import text
    import json  # in case you want to serialize extra_info later

    def end(self, record_count=None, status='success', extra_info=None):
        end_time = datetime.datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        update_query = text("""
            UPDATE etl_job_log
            SET end_time = :end_time,
                duration_seconds = :duration_seconds,
                record_count = :record_count,
                status = :status,
                extra_info = :extra_info
            WHERE id = :run_id
        """)

        values = {
            "end_time": end_time,
            "duration_seconds": int(duration),
            "record_count": record_count,
            "status": status,
            "extra_info": str(extra_info) if extra_info else None,
            "run_id": self.run_id
        }

        with self.engine.begin() as conn:
            conn.execute(update_query, values)

    # def fail(self, error_message):
    #     end_time = datetime.datetime.utcnow()
    #     duration = (end_time - self.start_time).total_seconds()
    #     update_query = """
    #         UPDATE etl_job_log
    #         SET end_time = %s,
    #             duration_seconds = %s,
    #             status = 'failed',
    #             error_message = %s
    #         WHERE id = %s
    #     """
    #     values = (
    #         end_time,
    #         int(duration),
    #         error_message[:1000],
    #         self.run_id
    #     )
    #     with self.engine.begin() as conn:
    #         conn.execute(update_query, values)

    def get_last_run(self, job_name=None, status=None, json_filters=None, extra_filters=None):
        job_name = job_name or self.job_name
        query = """
            SELECT * FROM etl_job_log
            WHERE job_name = :job_name
        """
        params = {"job_name": job_name}

        if status:
            query += " AND status = :status"
            params["status"] = status

        # Add support for extra filters
        if extra_filters:
            for key, value in extra_filters.items():
                query += f" AND {key} = :{key}"
                params[key] = value

        if json_filters:
            for key, value in json_filters.items():
                query += f" AND JSON_EXTRACT(CAST(run_parameters AS JSON), '$.{key})' = ':{key}'"
                params[key] = value

        query += " ORDER BY start_time DESC"  # Do we need Limit 1 if we are fetchone() below
        # query += " ORDER BY start_time DESC LIMIT 1"
        print(query)
        print(params)

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params).fetchone()
            if result:
                row_dict = dict(result._mapping)  # SQLAlchemy 1.4+ safe access
                # Parse JSON parameters
                raw_params = row_dict.get("run_parameters")
                parsed_params = {}
                try:
                    parsed_params = json.loads(raw_params) if raw_params else {}
                except json.JSONDecodeError:
                    print("⚠️ Could not parse run_parameters as JSON.")

                return row_dict, parsed_params
            else:
                return None, {}


    def fail(self, error_message):
        end_time = datetime.datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        update_query = text("""
            UPDATE etl_job_log
            SET end_time = :end_time,
                duration_seconds = :duration_seconds,
                status = 'failed',
                error_message = :error_message
            WHERE id = :run_id
        """)

        values = {
            "end_time": end_time,
            "duration_seconds": int(duration),
            "error_message": error_message[:1000],  # truncate if needed
            "run_id": self.run_id
        }

        with self.engine.begin() as conn:
            conn.execute(update_query, values)

