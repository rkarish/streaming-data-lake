#!/usr/bin/env python3
"""Architecture diagrams for the AdTech Data Playground.

Generates two PNGs in this directory:
  - adtech_architecture_application.png  (Application mode)
  - adtech_architecture_session.png      (Session mode)

Prerequisites: pip install diagrams && brew install graphviz
"""

import os

from diagrams import Cluster, Diagram, Edge
from diagrams.generic.database import SQL
from diagrams.generic.storage import Storage
from diagrams.k8s.compute import Deploy, Job, Pod
from diagrams.onprem.analytics import Flink, Trino
from diagrams.onprem.client import Client
from diagrams.onprem.database import Postgresql
from diagrams.onprem.gitops import Argocd
from diagrams.onprem.queue import Kafka
from diagrams.onprem.vcs import Gitea

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

GRAPH_ATTR = {
    "fontsize": "28",
    "bgcolor": "white",
    "pad": "0.4",
    "nodesep": "0.6",
    "ranksep": "0.8",
    "overlap": "false",
}

EDGE_ATTR = {"minlen": "1"}
NODE_ATTR = {"margin": "0.2"}


def build_diagram(mode):
    filename = os.path.join(SCRIPT_DIR, f"adtech_architecture_{mode}")

    with Diagram(
        "",
        filename=filename,
        show=False,
        direction="TB",
        outformat="png",
        graph_attr=GRAPH_ATTR,
        edge_attr=EDGE_ATTR,
        node_attr=NODE_ATTR,
    ):
        # ── Docker Compose Services ──────────────────────────────────────
        with Cluster("Docker Compose"):
            with Cluster("Data Generation"):
                datagen = Client("Mock Data\nGenerator")

            with Cluster("Message Bus"):
                kafka = Kafka("Kafka (KRaft)\nAvro / 4 topics")
                schema_reg = Kafka("Schema\nRegistry")

            with Cluster("Lakehouse Storage"):
                iceberg_rest = SQL("Iceberg REST\nCatalog")
                minio = Storage("MinIO\n(S3-compatible)")

            with Cluster("Query & Visualization"):
                trino = Trino("Trino")
                cloudbeaver = SQL("CloudBeaver\nSQL IDE")
                superset = SQL("Superset")
                superset_pg = Postgresql("Superset\nMetadata")

            gitea = Gitea("Gitea")

        # ── Kubernetes ───────────────────────────────────────────────────
        with Cluster("Kubernetes"):
            with Cluster("Operators & GitOps"):
                argocd = Argocd("Argo CD")
                flink_op = Deploy("Flink\nOperator")

            if mode == "application":
                flink_jobs = []
                for name in ["Ingestion", "Aggregation", "Funnel"]:
                    with Cluster(f"{name} Cluster"):
                        jm = Pod(f"JobManager")
                        tm = Pod(f"TaskManager\n(scalable)")
                        fj = Flink(name)
                        jm >> Edge(color="steelblue") >> tm
                        flink_jobs.append(fj)
            else:
                with Cluster("Session Cluster"):
                    session_jm = Pod("JobManager")
                    session_tm = Pod("TaskManager\n(8 slots)")
                    session_jm >> Edge(color="steelblue") >> session_tm

                with Cluster("SQL Submitter Jobs"):
                    job_ingest = Job("ingestion-job")
                    job_agg = Job("aggregation-job")
                    job_funnel = Job("funnel-job")
                    flink_jobs = [job_ingest, job_agg, job_funnel]

                for fj in flink_jobs:
                    fj >> Edge(color="steelblue", style="dashed") >> session_jm

        # ── Data pipeline edges (blue) ───────────────────────────────────
        datagen >> Edge(color="darkblue") >> kafka
        datagen >> Edge(color="darkblue", style="dashed") >> schema_reg

        for fj in flink_jobs:
            kafka >> Edge(color="darkgreen") >> fj

        # ── Flink → Iceberg (orange) ─────────────────────────────────────
        for fj in flink_jobs:
            fj >> Edge(color="darkorange") >> iceberg_rest

        iceberg_rest >> Edge(color="darkorange") >> minio

        # ── Query layer edges (purple) ───────────────────────────────────
        trino - Edge(color="purple", dir="both") - iceberg_rest
        cloudbeaver - Edge(color="mediumpurple", dir="both") - trino
        superset >> Edge(color="mediumpurple") >> trino
        superset >> Edge(color="gray") >> superset_pg

        # ── GitOps control plane edges (red, dashed) ─────────────────────
        gitea >> Edge(color="firebrick", style="dashed") >> argocd
        argocd >> Edge(color="firebrick", style="dashed") >> flink_op
        for fj in flink_jobs:
            flink_op >> Edge(color="firebrick", style="dashed") >> fj


if __name__ == "__main__":
    build_diagram("application")
    build_diagram("session")
