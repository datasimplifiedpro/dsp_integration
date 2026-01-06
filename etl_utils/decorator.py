from .logger import ETLLogger
import traceback

def log_etl_job(job_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # engine = kwargs.get("engine")
            parameters = kwargs.get("parameters", {})
            logger = ETLLogger(job_name)
            run_id, start_time = logger.start(parameters=parameters)
            try:
                result = func(*args, **kwargs, run_id=run_id, start_time=start_time)
                logger.end(record_count=result.get('record_count') if result else None)
                return result
            except Exception as e:
                logger.fail(traceback.format_exc())
                raise e
        return wrapper
    return decorator
