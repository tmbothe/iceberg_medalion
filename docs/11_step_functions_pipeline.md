# Step Functions Pipeline — Orchestration

## Table of Contents
1. [Why Step Functions?](#1-why-step-functions)
2. [Pipeline Structure (ASL Walkthrough)](#2-pipeline-structure-asl-walkthrough)
3. [State-by-State Explanation](#3-state-by-state-explanation)
4. [Default Parameters with States.JsonMerge](#4-default-parameters-with-statesjsonmerge)
5. [ResultPath: null — Preserving Input Through States](#5-resultpath-null--preserving-input-through-states)
6. [Triggering the Pipeline](#6-triggering-the-pipeline)
7. [Monitoring Executions](#7-monitoring-executions)
8. [Error Handling and Retries](#8-error-handling-and-retries)

---

## 1. Why Step Functions?

The 7 Glue jobs must run in a specific order:
- Bronze jobs (1–5) can run **in parallel** — they are independent
- Silver job (6) must run **after all Bronze jobs complete**
- Gold job (7) must run **after Silver completes**

Manually triggering jobs and waiting between steps is error-prone. AWS Step Functions
provides a managed, serverless workflow engine that:

| Feature | Benefit |
|---------|---------|
| Visual workflow graph | See exactly what's running and where it failed |
| Parallel execution | Run 5 Bronze jobs simultaneously, halving total time |
| Sync integration with Glue | `.sync` suffix waits for Glue job completion before proceeding |
| Automatic retries | Configurable retry logic on job failures |
| Execution history | Full audit trail of every pipeline run |
| Input/output passing | JSON state is threaded through all states |
| No server management | Fully serverless, pay per state transition |

---

## 2. Pipeline Structure (ASL Walkthrough)

The pipeline is defined in `infrastructure/stepfunctions/pipeline_definition.json`
using Amazon States Language (ASL).

```
SetDefaults (Pass)
    │
    ▼
ExtractParams (Pass)
    │
    ▼
BronzeIngestion (Parallel)
    ├── BronzeOperator (Task → Glue)
    ├── BronzeMachine  (Task → Glue)
    ├── BronzeMaterial (Task → Glue)
    ├── BronzeSupplier (Task → Glue)
    └── BronzeOplog    (Task → Glue)
    │    (all 5 run simultaneously)
    │    (wait for all to complete)
    ▼
SilverTransformation (Task → Glue)
    │
    ▼
GoldAggregation (Task → Glue)
    │
    ▼
END
```

**Total wall-clock time:** ~15–20 minutes for this dataset
- Bronze (parallel): ~5–8 minutes
- Silver: ~5–8 minutes
- Gold: ~3–5 minutes

---

## 3. State-by-State Explanation

### SetDefaults (Pass state)

```json
"SetDefaults": {
    "Type": "Pass",
    "Parameters": {
        "params.$": "States.JsonMerge(
            States.StringToJson('{\"load_batch\":\"data_2\",\"load_type\":\"incremental\",...}'),
            $,
            false
        )"
    },
    "Next": "ExtractParams"
}
```

**What it does:** Merges hardcoded default values with the execution input (`$`).
- First argument: JSON object of defaults
- Second argument: `$` = the execution input (user-provided)
- `false` = shallow merge (top-level keys only)
- User's keys override defaults; missing keys fall back to defaults

**Why a Pass state?** Pass states execute instantly (no cost, no latency). They are ideal
for transforming or defaulting input data.

---

### ExtractParams (Pass state)

```json
"ExtractParams": {
    "Type": "Pass",
    "Parameters": {
        "load_batch.$":  "$.params.load_batch",
        "load_type.$":   "$.params.load_type",
        "report_date.$": "$.params.report_date",
        "bucket_name.$": "$.params.bucket_name"
    },
    "Next": "BronzeIngestion"
}
```

**What it does:** Flattens the `params` nested object to top-level keys.
After this state, the execution input looks like:
```json
{
    "load_batch":  "data_2",
    "load_type":   "incremental",
    "report_date": "2024-02-02",
    "bucket_name": "mfg-lakehouse-442042510197-us-east-1"
}
```
These top-level keys are what downstream Task states read with `"$.load_batch"` etc.

---

### BronzeIngestion (Parallel state)

```json
"BronzeIngestion": {
    "Type": "Parallel",
    "Branches": [
        { "StartAt": "BronzeOperator", "States": { "BronzeOperator": { ... } } },
        { "StartAt": "BronzeMachine",  "States": { "BronzeMachine":  { ... } } },
        ...
    ],
    "ResultPath": null,
    "Next": "SilverTransformation"
}
```

**What it does:** Executes all 5 branches simultaneously. Waits until **all** branches
complete before moving to `SilverTransformation`.

**`ResultPath: null`** is critical — it discards the output of the Parallel state
(an array of Glue job run results) and keeps the original execution input for the next state.
Without this, the input to `SilverTransformation` would be the array of Glue outputs,
not the original `load_batch`/`load_type`/etc.

Each branch runs one Glue job:
```json
"BronzeOperator": {
    "Type": "Task",
    "Resource": "arn:aws:states:::glue:startJobRun.sync",
    "Parameters": {
        "JobName": "mfg-raw-to-bronze-operator",
        "Arguments": {
            "--LOAD_BATCH.$": "$.load_batch",
            "--BUCKET_NAME.$": "$.bucket_name"
        }
    },
    "End": true
}
```

The `.sync` in the resource ARN means Step Functions waits for the Glue job to complete
(and gets the final job run status) rather than just triggering it and moving on.

---

### SilverTransformation (Task state)

```json
"SilverTransformation": {
    "Type": "Task",
    "Resource": "arn:aws:states:::glue:startJobRun.sync",
    "Parameters": {
        "JobName": "mfg-bronze-to-silver",
        "Arguments": {
            "--LOAD_BATCH.$": "$.load_batch",
            "--LOAD_TYPE.$":  "$.load_type",
            "--BUCKET_NAME.$": "$.bucket_name"
        }
    },
    "ResultPath": null,
    "Next": "GoldAggregation"
}
```

Passes `LOAD_BATCH`, `LOAD_TYPE`, and `BUCKET_NAME` to the Silver Glue job.
`ResultPath: null` again discards Glue's output and preserves the original input
for `GoldAggregation`.

---

### GoldAggregation (Task state)

```json
"GoldAggregation": {
    "Type": "Task",
    "Resource": "arn:aws:states:::glue:startJobRun.sync",
    "Parameters": {
        "JobName": "mfg-silver-to-gold",
        "Arguments": {
            "--REPORT_DATE.$": "$.report_date",
            "--BUCKET_NAME.$": "$.bucket_name"
        }
    },
    "End": true
}
```

Last state in the pipeline. Note that Gold only needs `REPORT_DATE` and `BUCKET_NAME`
(not `LOAD_BATCH` or `LOAD_TYPE` — Gold reads from Silver which already has all history).

---

## 4. Default Parameters with States.JsonMerge

### The Problem

When you start a Step Functions execution with `{}` (empty input), states that reference
`"$.load_batch"` will fail with:
```
JSONPath '$.load_batch' could not be found in input '{}'
```

### The Solution

`States.JsonMerge` merges two JSON objects:

```
States.JsonMerge(
    {"load_batch":"data_2", "load_type":"incremental", ...},   ← defaults
    {},                                                         ← user input (empty)
    false                                                       ← shallow merge
)
= {"load_batch":"data_2", "load_type":"incremental", ...}      ← defaults win
```

When user provides some keys:
```
States.JsonMerge(
    {"load_batch":"data_2", "load_type":"incremental", ...},    ← defaults
    {"load_type":"initial", "load_batch":"data_1"},             ← user overrides
    false
)
= {"load_batch":"data_1", "load_type":"initial", ...}          ← user wins
```

**Default values used when not overridden:**

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `load_batch` | `data_2` | Use the incremental data |
| `load_type` | `incremental` | Run SCD2 incremental logic |
| `report_date` | `2024-02-02` | Aggregate up to this date |
| `bucket_name` | `mfg-lakehouse-442042510197-us-east-1` | Target S3 bucket |

### Why Two Pass States Instead of One?

`States.JsonMerge` returns an object. You cannot access a field of that object in the
same state with `$.params.load_batch.$` — that syntax is not valid.

The two-state pattern solves this:
1. **SetDefaults:** compute the merged `params` object
2. **ExtractParams:** access `$.params.load_batch`, `$.params.load_type`, etc.

---

## 5. ResultPath: null — Preserving Input Through States

**Problem:** A Task state (like a Glue job) produces output. By default, Step Functions
replaces the entire state input with the task output before passing to the next state.

A Glue job's output looks like:
```json
{"JobRunId": "jr_abc123", "JobName": "...", "StartedOn": "..."}
```

If the Silver state received this instead of `{"load_batch": "data_2", ...}`, it would
fail because `$.load_batch` doesn't exist in the Glue job output.

**Solution:** `"ResultPath": null` tells Step Functions to discard the task output and
pass the original input unchanged to the next state.

```
Input to BronzeIngestion:  {"load_batch": "data_2", ...}
BronzeIngestion produces:  [array of Glue job results]
ResultPath: null            → discard the array
Output to SilverTransformation: {"load_batch": "data_2", ...}  ← original input preserved
```

---

## 6. Triggering the Pipeline

### Default Run (incremental load with data_2)

```bash
aws stepfunctions start-execution \
    --state-machine-arn "arn:aws:states:us-east-1:442042510197:stateMachine:Manufacturing_Lakehouse_ETL_Pipeline" \
    --input '{}'
```

### Initial Load (data_1)

```bash
aws stepfunctions start-execution \
    --state-machine-arn "arn:aws:states:us-east-1:442042510197:stateMachine:Manufacturing_Lakehouse_ETL_Pipeline" \
    --input '{"load_batch": "data_1", "load_type": "initial"}'
```

### Incremental Load with Custom Date

```bash
aws stepfunctions start-execution \
    --state-machine-arn "arn:aws:states:us-east-1:442042510197:stateMachine:Manufacturing_Lakehouse_ETL_Pipeline" \
    --input '{"load_batch": "data_2", "load_type": "incremental", "report_date": "2024-03-15"}'
```

### Deploy Latest Definition

After editing `infrastructure/stepfunctions/pipeline_definition.json`:
```bash
bash scripts/4_deploy_stepfunctions.sh
```

---

## 7. Monitoring Executions

### AWS Console

1. Go to **AWS Step Functions → State machines → Manufacturing_Lakehouse_ETL_Pipeline**
2. Click an execution to see the visual workflow graph
3. States turn green (success) or red (failure) in real time
4. Click any state to see its input, output, and error details

### AWS CLI

```bash
# List recent executions
aws stepfunctions list-executions \
    --state-machine-arn "arn:aws:states:us-east-1:442042510197:stateMachine:Manufacturing_Lakehouse_ETL_Pipeline" \
    --query 'executions[].{Status:status,Name:name,Start:startDate}' \
    --max-results 5

# Get execution status
aws stepfunctions describe-execution \
    --execution-arn "arn:aws:states:us-east-1:442042510197:execution:Manufacturing_Lakehouse_ETL_Pipeline:<execution-id>" \
    --query '{Status:status,Start:startDate,Stop:stopDate}'

# Get full execution history (all events)
aws stepfunctions get-execution-history \
    --execution-arn "arn:aws:states:us-east-1:442042510197:execution:Manufacturing_Lakehouse_ETL_Pipeline:<execution-id>" \
    --query 'events[?type==`TaskStateExited`].{state:stateExitedEventDetails.name}'
```

---

## 8. Error Handling and Retries

The current pipeline has basic retry configuration on Glue tasks (`"MaxRetries": 1` in
the Glue job definition). For production use, consider adding Step Functions-level retries:

```json
"BronzeOperator": {
    "Type": "Task",
    "Resource": "arn:aws:states:::glue:startJobRun.sync",
    "Parameters": { ... },
    "Retry": [
        {
            "ErrorEquals": ["Glue.AWSGlueException", "States.TaskFailed"],
            "IntervalSeconds": 60,
            "MaxAttempts": 2,
            "BackoffRate": 2.0
        }
    ],
    "Catch": [
        {
            "ErrorEquals": ["States.ALL"],
            "Next": "NotifyFailure",
            "ResultPath": "$.error"
        }
    ],
    "End": true
}
```

| Configuration | Value | Meaning |
|---------------|-------|---------|
| `IntervalSeconds` | 60 | Wait 60s before first retry |
| `MaxAttempts` | 2 | Retry up to 2 times |
| `BackoffRate` | 2.0 | Double the wait time on each retry (60s, 120s) |
| `Catch` | `States.ALL` → NotifyFailure | After all retries fail, route to a notification state |

A `NotifyFailure` state could send an SNS notification, log to CloudWatch, or trigger
a remediation workflow.
