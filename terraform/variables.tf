#variable
variable "s3-bucket" {
  default = "nerdward-github-archive"
}

variable "redshift_name" {
  default = "redshift_1"
}


# Roles to input to the CreateEMROperator in airflow
variable "EMR_ROLE" {
  default = "EMR_DefaultRole"
}

variable "EMR_EC2_ROLE" {
  default = "EMR_EC2_DefaultRole"
}

variable "username" {
  description = "The string for the redshift DB master user"
  type = string
}

variable "password" {
  description = "The password must contain alphabets and numbers"
  type = string
}