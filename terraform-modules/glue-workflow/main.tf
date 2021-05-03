resource "aws_glue_workflow" "this" {
  name = "${var.project-name}-${var.module-name}"
}

resource "aws_glue_trigger" "ingestion" {
  name          = "${var.project-name}-${var.module-name}-trigger-ingestion"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.this.name

  actions {
    job_name = "${var.project-name}-${var.module-name}-data-ingestion"
  }
}

resource "aws_glue_trigger" "ingestion-crawler-update" {
  name          = "${var.project-name}-${var.module-name}-data-ingestion-crawler-trigger"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.this.name

  actions {
    crawler_name = "${var.project-name}-${var.module-name}-data-ingestion-crawler"
  }

  predicate {
    conditions {
      job_name = "${var.project-name}-${var.module-name}-data-ingestion"
      state    = "SUCCEEDED"
    }
  }
}

resource "aws_glue_trigger" "transformation" {
  name          = "${var.project-name}-${var.module-name}-trigger-transformation"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.this.name

  predicate {
    conditions {
      crawler_name = "${var.project-name}-${var.module-name}-data-ingestion-crawler"
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = "${var.project-name}-${var.module-name}-data-transformation"
  }
}

resource "aws_glue_trigger" "transformation-crawler-update" {
  name          = "${var.project-name}-${var.module-name}-data-transformation-crawler-trigger"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.this.name

  actions {
    crawler_name = "${var.project-name}-${var.module-name}-data-transformation-crawler"
  }

  predicate {
    conditions {
      job_name = "${var.project-name}-${var.module-name}-data-transformation"
      state    = "SUCCEEDED"
    }
  }
}