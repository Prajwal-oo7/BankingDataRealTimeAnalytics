from airflow.utils.email import send_email


def notify_failure(context):

    task = context["task_instance"]
    dag = context["dag"]
    execution_date = context["execution_date"]

    subject = f"Airflow Failure: {dag.dag_id}.{task.task_id}"

    body = f"""
    <h3>Pipeline Failed</h3>
    <p><b>DAG:</b> {dag.dag_id}</p>
    <p><b>Task:</b> {task.task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Log:</b> <a href="{task.log_url}">View Logs</a></p>
    """

    send_email(
        to=["7171iron@gmail.com"],
        subject=subject,
        html_content=body
    )




def notify_pipeline_success(**context):

    dag_run = context["dag_run"]

    send_email(
        to=["7171iron@gmail.com"],
        subject=f"✅ Banking Pipeline Completed Successfully ({dag_run.run_id})",
        html_content=f"""
        <h2>Pipeline Completed Successfully</h2>

        <b>DAG:</b> {dag_run.dag_id}<br>
        <b>Run ID:</b> {dag_run.run_id}<br>
        <b>Execution Date:</b> {context["execution_date"]}<br>

        <br>

        All pipeline tasks completed successfully.

        <br><br>

        Airflow UI:
        <a href="{context['task_instance'].log_url}">
        View Logs
        </a>
        """
    )