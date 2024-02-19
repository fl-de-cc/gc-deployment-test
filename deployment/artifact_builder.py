from datetime import timedelta
import json
from typing import Optional
from google.cloud import dataproc_v1 as dataproc
from google.cloud.dataproc_v1.types.workflow_templates import WorkflowTemplate, ParameterValidation
from google.cloud import scheduler_v1


def build_workflow_yaml(region: str, cluster_name: str, service_account: str, artifacts_bucket: str) -> dict:
    return {
        "jobs": [
            pyspark_job("job_id_1", artifacts_bucket),
            pyspark_job("job_id_2", artifacts_bucket, ["job_id_1"]),
        ],
        "placement": build_placement(region, cluster_name, service_account, artifacts_bucket)
    }


def pyspark_job(job_id: str, artifacts_bucket: str, job_dependencies: Optional[list[str]] = None) -> dict:
    return {
        "pyspark_job": {
            "main_python_file_uri": f"gs://{artifacts_bucket}/artifacts/job_execution.py",
        },
        "step_id": job_id,
        "prerequisite_step_ids": job_dependencies
    }


def build_placement(region: str, cluster_name: str, service_account: str, artifacts_bucket: str) -> dict:

    return {
        "managed_cluster": {
            "cluster_name": cluster_name,
            "config": {
                "gce_cluster_config": {
                    "zone_uri": "",
                    "internal_ip_only": True,
                    "service_account": service_account,
                    "service_account_scopes": [
                        "https://www.googleapis.com/auth/cloud-platform",
                        "https://www.googleapis.com/auth/drive"
                    ]
                },
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n2-standard-2",
                    "disk_config": {
                        "boot_disk_size_gb": 30,
                        "num_local_ssds": 1
                    }
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n2-standard-2",
                    "disk_config": {
                        "boot_disk_size_gb": 30,
                        "num_local_ssds": 1
                    }
                },
                "software_config": {
                    "image_version": "2.2.5-ubuntu22",

                    "properties": {
                        "spark:spark.jars.packages": ","
                        .join(["io.delta:delta-spark_2.12:3.1.0"]),
                        "spark:spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                        "spark:spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                        "spark:spark.jars": f"gs://{artifacts_bucket}/artifacts/pubsublite-spark-sql-streaming-1.0.0-with-dependencies.jar"
                    },
                },
            }
        }
    }


def create_workflow(
        region: str,
        project_id: str,
        cluster_name: str,
        service_account: str,
        artifacts_bucket: str,
        template_id: str,
):
    template_workflow = build_workflow_yaml(region, cluster_name, service_account, artifacts_bucket)

    print(f"WorkflowTemplate ... ")
    workflow_template = WorkflowTemplate(
        id=template_id,
        name=f"projects/{project_id}/regions/{region}/workflowTemplates/{template_id}",  # TODO IS IT REQUIRED?
        **template_workflow
    )

    # print(f"CreateWorkflowTemplateRequest ... ")
    # workflow_template_request =
    print(f"WorkflowTemplateServiceClient ... ")
    client = dataproc.WorkflowTemplateServiceClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    print(f"create_workflow_template ... ")
    client.create_workflow_template(
        request=dataproc.CreateWorkflowTemplateRequest(
            parent=f"projects/{project_id}/regions/{region}",
            template=workflow_template
        )
    )


def schedule_pipeline_execution(
        region: str,
        project_id: str,
        template_id: str,
        service_account: str,
):
    job_name = f"projects/{project_id}/locations/{region}/jobs/locally_created"
    client = scheduler_v1.CloudSchedulerClient()
    job = scheduler_v1.Job(
        name=job_name,
        http_target=scheduler_v1.HttpTarget(
            uri=f"https://dataproc.googleapis.com/v1/projects/{project_id}/regions/{region}/workflowTemplates/{template_id}:instantiate?alt=json",
            oauth_token = scheduler_v1.OAuthToken(service_account_email=service_account)
        ),
        schedule="33 19 * * *",
        time_zone="Europe/Madrid",
    )

    print("Deleting job ... ") # TODO Check first if exists...
    client.delete_job(scheduler_v1.DeleteJobRequest(name=job_name))
    print("Creating job ... ")
    response = client.create_job(
        scheduler_v1.CreateJobRequest(
            parent=client.common_location_path(project_id, region),
            job=job,
        )
    )
    print(f"{response}")  # TODO is this async?


if __name__ == "__main__":
    region = "europe-west3"
    project_id = "plasma-inquiry-413718" # "prod-recova-ai"
    cluster_name = "recova-test"
    service_account = "28831740628-compute@developer.gserviceaccount.com" # "gke-prim-dp-default@prod-recova-ai.iam.gserviceaccount.com"  #
    artifacts_bucket = "dataproc-staging-europe-west3-28831740628-ocrcx1tc" # "dataproc-staging-europe-west3-1074763801763-eggw1bmc"  #
    template_id = "template-4"

    create_workflow(
       region,
       project_id,
       cluster_name,
       service_account,
       artifacts_bucket,
       template_id,
    )

    schedule_pipeline_execution(
        region,
        project_id,
        template_id,
        service_account,
    )
