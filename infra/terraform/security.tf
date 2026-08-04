/*
  Security groups and IAM.

  Every data service is reachable only from inside the VPC, and specifically
  only from the security group that legitimately needs it. Referencing source
  security groups rather than CIDR blocks means the rules stay correct as
  subnets change, and it makes the intent readable: "the cluster may reach the
  database" rather than "10.20.16.0/20 may reach the database".
*/

# ── EKS workloads ─────────────────────────────────────────────────────────

resource "aws_security_group" "eks_workloads" {
  name        = "${local.name}-eks-workloads"
  description = "Pods running on the EKS node groups"
  vpc_id      = module.vpc.vpc_id

  egress {
    description = "Outbound to AWS services and the internet via NAT"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-eks-workloads" }
}

# ── OLTP database ─────────────────────────────────────────────────────────

resource "aws_security_group" "database" {
  name        = "${local.name}-database"
  description = "RDS Postgres. Reachable from EKS workloads and the Connect cluster only."
  vpc_id      = module.vpc.vpc_id

  tags = { Name = "${local.name}-database" }
}

resource "aws_security_group_rule" "database_from_workloads" {
  type                     = "ingress"
  description              = "Application and Debezium connections"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.database.id
  source_security_group_id = aws_security_group.eks_workloads.id
}

# No egress rule. The OLTP database has no reason to initiate outbound
# connections, and an empty egress set is the cheapest exfiltration control
# available.

# ── Kafka ─────────────────────────────────────────────────────────────────

resource "aws_security_group" "msk" {
  name        = "${local.name}-msk"
  description = "MSK brokers"
  vpc_id      = module.vpc.vpc_id

  tags = { Name = "${local.name}-msk" }
}

resource "aws_security_group_rule" "msk_tls_from_workloads" {
  type                     = "ingress"
  description              = "TLS clients (Flink, Connect, producers)"
  from_port                = 9094
  to_port                  = 9094
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk.id
  source_security_group_id = aws_security_group.eks_workloads.id
}

resource "aws_security_group_rule" "msk_iam_from_workloads" {
  type                     = "ingress"
  description              = "SASL/IAM authentication port"
  from_port                = 9098
  to_port                  = 9098
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk.id
  source_security_group_id = aws_security_group.eks_workloads.id
}

resource "aws_security_group_rule" "msk_internal" {
  type              = "ingress"
  description       = "Inter-broker replication"
  from_port         = 9090
  to_port           = 9098
  protocol          = "tcp"
  security_group_id = aws_security_group.msk.id
  self              = true
}

resource "aws_security_group_rule" "msk_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.msk.id
  cidr_blocks       = ["0.0.0.0/0"]
}

# ── Feature store ─────────────────────────────────────────────────────────

resource "aws_security_group" "redis" {
  name        = "${local.name}-redis"
  description = "ElastiCache for the online feature store"
  vpc_id      = module.vpc.vpc_id

  tags = { Name = "${local.name}-redis" }
}

resource "aws_security_group_rule" "redis_from_workloads" {
  type                     = "ingress"
  description              = "Recommendation API reads and Flink sink writes"
  from_port                = 6379
  to_port                  = 6379
  protocol                 = "tcp"
  security_group_id        = aws_security_group.redis.id
  source_security_group_id = aws_security_group.eks_workloads.id
}

resource "aws_elasticache_subnet_group" "features" {
  name       = "${local.name}-features"
  subnet_ids = module.vpc.private_subnets
}

# ── Warehouse ─────────────────────────────────────────────────────────────

resource "aws_security_group" "redshift" {
  name        = "${local.name}-redshift"
  description = "Redshift Serverless workgroup"
  vpc_id      = module.vpc.vpc_id

  tags = { Name = "${local.name}-redshift" }
}

resource "aws_security_group_rule" "redshift_from_workloads" {
  type                     = "ingress"
  description              = "dbt runs from Airflow workers on EKS"
  from_port                = 5439
  to_port                  = 5439
  protocol                 = "tcp"
  security_group_id        = aws_security_group.redshift.id
  source_security_group_id = aws_security_group.eks_workloads.id
}

resource "aws_security_group_rule" "redshift_egress" {
  type              = "egress"
  description       = "Reading Parquet from the S3 lake"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.redshift.id
  cidr_blocks       = ["0.0.0.0/0"]
}

# ── Logging ───────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = var.environment == "prod" ? 30 : 7
  kms_key_id        = aws_kms_key.platform.arn
}

# ── IAM ───────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "assume_role" {
  for_each = toset(["monitoring.rds.amazonaws.com", "redshift.amazonaws.com"])

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = [each.key]
    }
  }
}

resource "aws_iam_role" "rds_monitoring" {
  name               = "${local.name}-rds-monitoring"
  assume_role_policy = data.aws_iam_policy_document.assume_role["monitoring.rds.amazonaws.com"].json
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  role       = aws_iam_role.rds_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

resource "aws_iam_role" "redshift" {
  name               = "${local.name}-redshift"
  assume_role_policy = data.aws_iam_policy_document.assume_role["redshift.amazonaws.com"].json
}

# Scoped to the lake bucket rather than granting a managed S3 policy. Redshift
# needs to read Parquet from one prefix; there is no reason for it to be able
# to read every bucket in the account.
data "aws_iam_policy_document" "redshift_lake_access" {
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.platform.arn]
  }
}

resource "aws_iam_role_policy" "redshift_lake_access" {
  name   = "lake-read"
  role   = aws_iam_role.redshift.id
  policy = data.aws_iam_policy_document.redshift_lake_access.json
}

# ── IRSA for the recommendation API ───────────────────────────────────────
# Pod-level credentials. No AWS keys exist in the cluster to leak or rotate.

data "aws_iam_policy_document" "recommendations_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:sub"
      values   = ["system:serviceaccount:serving:recommendations"]
    }
  }
}

resource "aws_iam_role" "recommendations" {
  name               = "${local.name}-recommendations"
  assume_role_policy = data.aws_iam_policy_document.recommendations_assume.json
}

data "aws_iam_policy_document" "model_read" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.models.arn, "${aws_s3_bucket.models.arn}/*"]
  }
}

resource "aws_iam_role_policy" "recommendations_model_read" {
  name   = "model-read"
  role   = aws_iam_role.recommendations.id
  policy = data.aws_iam_policy_document.model_read.json
}

resource "aws_s3_bucket" "models" {
  bucket = "${local.name}-models"
}

resource "aws_s3_bucket_versioning" "models" {
  bucket = aws_s3_bucket.models.id
  # Versioned so a bad model promotion can be reverted to the exact prior
  # artefact rather than retrained from scratch.
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "models" {
  bucket                  = aws_s3_bucket.models.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
