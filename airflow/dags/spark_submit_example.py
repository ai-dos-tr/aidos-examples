from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

SPARK_IMAGE = "quay.io/aidos/images/spark-executor:4.1.2-iceberg-1.11.0-2"

SPARK_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "spark-pi-{{ ts_nodash | lower }}",
        "namespace": "default",
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": SPARK_IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
        "arguments": ["1000"],
        "sparkVersion": "4.1.2",
        "restartPolicy": {"type": "Never"},
        "sparkConf": {
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "s3a://spark-events/event-logs/",
            "spark.hadoop.fs.s3a.endpoint": "https://seaweedfs-default.iett.gov.tr",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            # This platform's internal services (seaweedfs, keycloak, ...) use a
            # self-signed CA the JVM doesn't trust by default -- without this, any
            # HTTPS call (S3A here) fails with "PKIX path building failed". Same
            # cert bundle/truststore jupyterhub's Spark profiles already mount.
            "spark.driver.extraJavaOptions": "-Djavax.net.ssl.trustStore=/cacerts/bundle.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=",
            "spark.executor.extraJavaOptions": "-Djavax.net.ssl.trustStore=/cacerts/bundle.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=",
        },
        "volumes": [
            {"name": "cacerts", "secret": {"secretName": "certs-bundle"}},
        ],
        "driver": {
            "cores": 1,
            "memory": "1g",
            "serviceAccount": "spark",
            "envSecretKeyRefs": {
                "AWS_ACCESS_KEY_ID": {"name": "creds-examples-s3", "key": "S3_ACCESS_KEY"},
                "AWS_SECRET_ACCESS_KEY": {"name": "creds-examples-s3", "key": "S3_SECRET_KEY"},
            },
            "volumeMounts": [{"name": "cacerts", "mountPath": "/cacerts", "readOnly": True}],
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "1g",
            "envSecretKeyRefs": {
                "AWS_ACCESS_KEY_ID": {"name": "creds-examples-s3", "key": "S3_ACCESS_KEY"},
                "AWS_SECRET_ACCESS_KEY": {"name": "creds-examples-s3", "key": "S3_SECRET_KEY"},
            },
            "volumeMounts": [{"name": "cacerts", "mountPath": "/cacerts", "readOnly": True}],
        },
    },
}

with DAG(
    dag_id="submit_spark_pi",
    description="Submits a SparkPi job via the platform's spark-operator",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "example"],
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_spark_pi",
        namespace="default",
        template_spec=SPARK_APPLICATION,
        kubernetes_conn_id="kubernetes_default",
    )

    # The app name is deterministic from the template (same Jinja expression as
    # SPARK_APPLICATION["metadata"]["name"]), so the sensor doesn't need to xcom_pull
    # it from the submit task -- do_xcom_push=True on that task makes it hang forever
    # waiting for an xcom-sidecar container that's never configured on the driver pod.
    monitor = SparkKubernetesSensor(
        task_id="monitor_spark_pi",
        namespace="default",
        application_name="spark-pi-{{ ts_nodash | lower }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
    )

    submit >> monitor