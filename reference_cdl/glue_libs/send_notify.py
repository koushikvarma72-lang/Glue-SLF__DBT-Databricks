"""send_notify — reconstructed notification helpers for the reference CDL Glue jobs.

Call surface (from the job scripts):
    send_error_notify(TopicArn, workflow_name, job_name, source_system)
    send_job_failure_notify(TopicArn, workflow_name, job_name, source_system, status_result)

Both are NON-FATAL: notification problems (dummy topic, no SNS perms in the demo
account) log a warning and return — they never fail the pipeline.
"""

from dl_common_lib_function import get_dl_common_functions as dl_lib

logger = dl_lib.initiate_logger()


def _publish(topic_arn, message, subject):
    dl_lib.send_sns_notify(topic_arn, message, subject)


def send_error_notify(TopicArn, workflow_name, job_name, source_system):
    message = (f"Hi Team,\n\nAn error occurred in workflow '{workflow_name}', "
               f"job '{job_name}'.\nSource_System: '{source_system}'\n\n"
               "Kindly check the job logs.\n\nRegards,\nCOMPANY DL Team")
    _publish(TopicArn, message, f" Notification: Job Error - {source_system}")


def send_job_failure_notify(TopicArn, workflow_name, job_name, source_system, status_result):
    """status_result may be a string, a list of result dicts, or a list of lists —
    only FAILED entries trigger a notification."""
    failures = []
    def _walk(item):
        if isinstance(item, dict):
            if str(item.get("status", "")).upper() == "FAILED":
                failures.append(item.get("message") or item.get("file_name") or str(item)[:120])
        elif isinstance(item, (list, tuple)):
            for sub in item:
                _walk(sub)
        elif isinstance(item, str) and item:
            failures.append(item[:200])
    _walk(status_result)
    if not failures:
        return
    body = "\n".join(f"- {f}" for f in failures[:20])
    message = (f"Hi Team,\n\nFailures in workflow '{workflow_name}', job '{job_name}' "
               f"(source_system '{source_system}'):\n\n{body}\n\n"
               "Kindly check the job logs.\n\nRegards,\nCOMPANY DL Team")
    _publish(TopicArn, message, f" Notification: Job Failure - {source_system}")
    logger.info("send_job_failure_notify: %d failure(s) reported", len(failures))
