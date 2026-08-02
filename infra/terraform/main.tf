terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
  }

  # State in S3 with DynamoDB locking. Local state on a platform this size is a
  # single laptop failure away from an unmanageable environment.
  backend "s3" {
    bucket         = "streaming-platform-tfstate"
    key            = "platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "realtime-personalization"
      Environment = var.environment
      ManagedBy   = "terraform"
      CostCenter  = "data-platform"
    }
  }
}

locals {
  name = "streaming-${var.environment}"

  # Three AZs, not two. MSK and RDS multi-AZ both need it, and a two-AZ cluster
  # loses quorum when one zone goes.
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
}

data "aws_availability_zones" "available" {
  state = "available"
}

# ── Network ───────────────────────────────────────────────────────────────

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = "${local.name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for i in range(3) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i in range(3) : cidrsubnet(var.vpc_cidr, 8, i + 48)]
  # Data services get their own tier with no route to the internet at all.
  database_subnets = [for i in range(3) : cidrsubnet(var.vpc_cidr, 8, i + 51)]

  enable_nat_gateway = true
  # One NAT gateway per AZ in production: a single shared gateway is both a
  # cross-AZ data transfer charge and a zone-level single point of failure.
  single_nat_gateway = var.environment != "prod"
  one_nat_gateway_per_az = var.environment == "prod"

  enable_dns_hostnames = true
  enable_dns_support   = true

  # S3 traffic stays on the AWS backbone. On this event volume the gateway
  # endpoint pays for itself many times over in avoided NAT charges.
  enable_s3_endpoint = true

  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }
}

# ── OLTP database ─────────────────────────────────────────────────────────

resource "aws_db_parameter_group" "postgres" {
  name   = "${local.name}-pg16"
  family = "postgres16"

  # Logical replication is what Debezium reads. It cannot be enabled without a
  # reboot, so it belongs in the parameter group from day one.
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_replication_slots"
    value        = "10"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_wal_senders"
    value        = "10"
    apply_method = "pending-reboot"
  }

  # An abandoned replication slot holds WAL forever and eventually fills the
  # volume, taking the database down. This caps the damage at 32 GB.
  parameter {
    name  = "max_slot_wal_keep_size"
    value = "32768"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }
}

resource "aws_db_instance" "oltp" {
  identifier     = "${local.name}-oltp"
  engine         = "postgres"
  engine_version = "16.3"
  instance_class = var.db_instance_class

  allocated_storage     = 100
  max_allocated_storage = 1000  # storage autoscaling; running out of disk is not an outage worth having
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "streaming"
  username = "platform_admin"
  # Password lives in Secrets Manager and rotates; it is never in state or code.
  manage_master_user_password = true

  parameter_group_name   = aws_db_parameter_group.postgres.name
  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.database.id]

  multi_az                = var.environment == "prod"
  backup_retention_period = var.environment == "prod" ? 30 : 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:30-sun:05:30"

  performance_insights_enabled = true
  monitoring_interval          = 30
  monitoring_role_arn          = aws_iam_role.rds_monitoring.arn
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  deletion_protection      = var.environment == "prod"
  skip_final_snapshot      = var.environment != "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${local.name}-final-${formatdate("YYYYMMDDhhmm", timestamp())}" : null

  # Applying a change that requires a reboot during business hours is a choice,
  # not an accident.
  apply_immediately = false

  lifecycle {
    ignore_changes = [final_snapshot_identifier]
  }
}

# ── Kafka ─────────────────────────────────────────────────────────────────

resource "aws_msk_cluster" "events" {
  cluster_name           = "${local.name}-events"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.msk_volume_size

        # Broker storage fills silently and then the cluster stops accepting
        # writes. Autoscaling turns that incident into a non-event.
        provisioned_throughput {
          enabled           = true
          volume_throughput = 250
        }
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = aws_kms_key.platform.arn
  }

  client_authentication {
    sasl {
      iam = true  # IAM auth rather than SCRAM: no passwords to rotate or leak
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.events.arn
    revision = aws_msk_configuration.events.latest_revision
  }

  open_monitoring {
    prometheus {
      jmx_exporter { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}

resource "aws_msk_configuration" "events" {
  name           = "${local.name}-events-config"
  kafka_versions = ["3.6.0"]

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=12
    log.retention.hours=168
    log.retention.bytes=-1
    compression.type=zstd
    unclean.leader.election.enable=false
  PROPS

  # min.insync.replicas=2 with acks=all is the pair that actually guarantees
  # durability. Either one alone does not: acks=all with ISR of 1 acknowledges
  # a write that a single broker failure loses.
}

# ── Kubernetes ────────────────────────────────────────────────────────────

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8"

  cluster_name    = "${local.name}-eks"
  cluster_version = "1.29"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access       = var.environment != "prod"
  cluster_endpoint_private_access      = true
  cluster_endpoint_public_access_cidrs = var.admin_cidrs

  enable_irsa = true  # pod-level IAM, so no node-wide credentials for workloads

  cluster_addons = {
    coredns                = { most_recent = true }
    kube-proxy             = { most_recent = true }
    vpc-cni                = { most_recent = true }
    aws-ebs-csi-driver     = { most_recent = true }
  }

  eks_managed_node_groups = {
    # Stateless request-serving. Scales with traffic.
    serving = {
      instance_types = ["m6i.xlarge"]
      min_size       = 3
      max_size       = 20
      desired_size   = 4
      labels         = { workload = "serving" }
    }

    # Flink task managers. Memory-heavy because of RocksDB state, and tainted
    # so that a burst of API pods cannot evict a streaming job mid-checkpoint.
    streaming = {
      instance_types = ["r6i.2xlarge"]
      min_size       = 3
      max_size       = 12
      desired_size   = 3
      labels         = { workload = "streaming" }
      taints = [{
        key    = "workload"
        value  = "streaming"
        effect = "NO_SCHEDULE"
      }]
    }

    # Batch and training. Spot, because these jobs are restartable and this is
    # roughly a 70% saving on the most expensive instance class in the cluster.
    batch = {
      instance_types = ["r6i.4xlarge", "r5.4xlarge", "r6a.4xlarge"]
      capacity_type  = "SPOT"
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      labels         = { workload = "batch" }
      taints = [{
        key    = "workload"
        value  = "batch"
        effect = "NO_SCHEDULE"
      }]
    }
  }
}

# ── Online feature store ──────────────────────────────────────────────────

resource "aws_elasticache_replication_group" "features" {
  replication_group_id = "${local.name}-features"
  description          = "Online feature store for the recommendation API"

  engine         = "redis"
  engine_version = "7.1"
  node_type      = var.redis_node_type
  port           = 6379

  num_node_groups         = var.environment == "prod" ? 3 : 1
  replicas_per_node_group = var.environment == "prod" ? 1 : 0

  automatic_failover_enabled = var.environment == "prod"
  multi_az_enabled           = var.environment == "prod"

  subnet_group_name  = aws_elasticache_subnet_group.features.name
  security_group_ids = [aws_security_group.redis.id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true

  parameter_group_name = aws_elasticache_parameter_group.features.name

  snapshot_retention_limit = 1
  maintenance_window       = "sun:05:00-sun:06:00"
}

resource "aws_elasticache_parameter_group" "features" {
  name   = "${local.name}-features"
  family = "redis7"

  # Features are a cache, not a database: under memory pressure, evict the
  # least recently used key rather than start rejecting writes. The streaming
  # job would otherwise back up and the whole pipeline would stall over a cache.
  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }
}

# ── Lake storage ──────────────────────────────────────────────────────────

resource "aws_s3_bucket" "lake" {
  bucket = "${local.name}-lake"
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "tier-raw-events"
    status = "Enabled"
    filter { prefix = "playback.events/" }

    # Raw events are queried heavily for two weeks, occasionally for three
    # months, and almost never after that. The tiering is chosen from the
    # observed access pattern, not from a default template.
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    expiration { days = 730 }

    # Multipart uploads from a failed Connect task otherwise accumulate as
    # invisible, billable storage.
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.platform.arn
    }
    bucket_key_enabled = true  # cuts KMS request charges substantially at this object count
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Warehouse ─────────────────────────────────────────────────────────────

resource "aws_redshiftserverless_namespace" "warehouse" {
  namespace_name      = "${local.name}-warehouse"
  db_name             = "analytics"
  admin_username      = "warehouse_admin"
  manage_admin_password = true
  kms_key_id          = aws_kms_key.platform.arn
  iam_roles           = [aws_iam_role.redshift.arn]

  log_exports = ["userlog", "connectionlog", "useractivitylog"]
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  namespace_name = aws_redshiftserverless_namespace.warehouse.namespace_name
  workgroup_name = "${local.name}-wg"

  # Serverless rather than provisioned: the warehouse is idle for eighteen
  # hours a day and busy for three. Paying for peak capacity around the clock
  # would roughly triple the bill for no benefit.
  base_capacity        = var.redshift_base_rpu
  max_capacity         = var.redshift_max_rpu
  publicly_accessible  = false
  subnet_ids           = module.vpc.private_subnets
  security_group_ids   = [aws_security_group.redshift.id]

  config_parameter {
    parameter_key   = "enable_user_activity_logging"
    parameter_value = "true"
  }
}

resource "aws_kms_key" "platform" {
  description             = "Encryption for ${local.name} data at rest"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}
