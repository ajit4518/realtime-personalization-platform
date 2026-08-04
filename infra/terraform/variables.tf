variable "aws_region" {
  description = "Region for all platform resources."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment. Drives multi-AZ, backup retention, and deletion protection."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC. Needs room for three private, three public, and three database subnets."
  type        = string
  default     = "10.20.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR block."
  }
}

variable "admin_cidrs" {
  description = "CIDRs permitted to reach the EKS public endpoint in non-production."
  type        = list(string)
  default     = []
}

# ── Sizing ────────────────────────────────────────────────────────────────
# Defaults are the smallest instance that holds the observed working set.
# Environment tfvars override upward for production.

variable "db_instance_class" {
  description = "RDS instance class. Must have headroom for the WAL that logical replication retains."
  type        = string
  default     = "db.t4g.medium"
}

variable "msk_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "msk_volume_size" {
  description = "Per-broker EBS in GB. Seven days retention at peak throughput plus 40% headroom."
  type        = number
  default     = 500
}

variable "redis_node_type" {
  description = "ElastiCache node type. Feature vectors are ~400 bytes; size for profiles x 1.5 for fragmentation."
  type        = string
  default     = "cache.r7g.large"
}

variable "redshift_base_rpu" {
  description = "Redshift Serverless baseline capacity. Below 8 the dbt build takes longer than its window."
  type        = number
  default     = 8

  validation {
    condition     = var.redshift_base_rpu >= 8 && var.redshift_base_rpu <= 512
    error_message = "redshift_base_rpu must be between 8 and 512."
  }
}

variable "redshift_max_rpu" {
  description = "Ceiling for Redshift Serverless. Caps the blast radius of a runaway query on the bill."
  type        = number
  default     = 64
}
