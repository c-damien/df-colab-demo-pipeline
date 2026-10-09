# Google Cloud Dataflow Streaming Pipelines Demonstration (GCS Sinks)

This self-contained notebook demonstrates two end-to-end **Apache Beam (Python)** streaming pipelines running on **Google Cloud Dataflow**:

1. **Pipeline 1: Pub/Sub $\rightarrow$ Dual Cloud Storage (GCS) Destinations (Branching Pipeline)**
   - Reads streaming messages once from a **Cloud Pub/Sub** topic (`beam.io.ReadFromPubSub`).
   - Branches the decoded `PCollection` into **two parallel paths** writing to **two different GCS destinations**:
     - **Branch A (Raw Archive Destination)**: Windows the unmodified messages into `FixedWindows` and writes JSONL files to `gs://<bucket>/pipeline1/raw_archive`.
     - **Branch B (Curated / Enriched Destination)**: Enriches the JSON payload with ingestion metadata (`ingested_at`, `pipeline_branch`), windows into `FixedWindows`, and writes JSONL files to `gs://<bucket>/pipeline1/curated_events`.

2. **Pipeline 2: Continuous GCS Folder Monitoring $\rightarrow$ JSON Mapping $\rightarrow$ Destination GCS Folder**
   - Continuously polls an input **GCS folder** (`gs://<bucket>/incoming/*.json`) for new files using `apache_beam.io.fileio.MatchContinuously` (with stateful file deduplication).
   - Reads each newly arrived file, parses JSON/JSONL payloads, applies a **JSON field mapping and enrichment transformation**, and routes any malformed records to a Dead-Letter Queue (DLQ) branch.
   - Writes the transformed JSON records into a **different GCS folder** (`gs://<bucket>/transformed/`).

---

### Notebook Structure
Each pipeline section is completely self-contained and includes:
- **User Parameters**: Interactive Colab form parameters (`PROJECT_ID`, `REGION`, `BUCKET_NAME`, `PUBSUB_TOPIC_NAME`, etc.).
- **Infrastructure Setup**: Ready-to-execute `gcloud` commands to create the GCS bucket and Pub/Sub topic.
- **Self-Contained Pipeline Code**: Standalone Python scripts written via `%%writefile` so Dataflow workers have clean module state.
- **Dataflow Job Launch**: Ready-to-execute commands to submit the streaming jobs to `DataflowRunner`.
- **Test Payload Generator**: Commands to publish sample events to Pub/Sub and upload sample JSON files to GCS to test the live pipelines.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://githubtocolab.com/c-damien/df-colab-demo-pipeline/blob/main/dataflow_pipelines_gcs_demo.ipynb)
