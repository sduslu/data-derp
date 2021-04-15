variable "bucket-name" {
  type = string
}

variable "bucket-acl" {
  default = "private"
}

variable "force-destroy" {
  description = "Force destroy even when there are objects in bucket"
  default     = false
}

variable "enable-versioning" {
  description = "Enable/Disable versioning"
  default     = true
}

variable "access-identifiers" {
  type = list
  default = null
}