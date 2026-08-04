output "vpc_id" {
  value = module.vpc.vpc_id
}

output "database_endpoint" {
  description = "RDS writer endpoint. Debezium and the application connect here."
  value       = aws_db_instance.oltp.endpoint
}

output "database_secret_arn" {
  description = "Secrets Manager ARN holding the rotating master password."
  value       = aws_db_instance.oltp.master_user_secret[0].secret_arn
  sensitive   = true
}

output "kafka_bootstrap_brokers" {
  description = "SASL/IAM bootstrap string for producers and Flink."
  value       = aws_msk_cluster.events.bootstrap_brokers_sasl_iam
}

output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "eks_configure_command" {
  description = "Run this to point kubectl at the cluster."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.aws_region}"
}

output "redis_primary_endpoint" {
  description = "Online feature store. Set as RECO_REDIS_URL on the API."
  value       = aws_elasticache_replication_group.features.primary_endpoint_address
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.warehouse.endpoint[0].address
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "model_bucket" {
  description = "Set as model.bucket in the Helm values."
  value       = aws_s3_bucket.models.id
}

output "recommendations_role_arn" {
  description = "IRSA role for the API service account. Goes in the Helm serviceAccount annotation."
  value       = aws_iam_role.recommendations.arn
}
