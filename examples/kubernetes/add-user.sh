#!/usr/bin/env bash
# Gives a Kyuubi user a namespace, spark-<user>, where the launcher runs their jobs (and where Kyuubi
# would put their Spark engine): Pod Security "baseline", a Role that lets Kyuubi's service account
# manage pods there (and nowhere else), and a quota on what the user can run at once.
#   examples/kubernetes/add-user.sh alice
# Put the Secrets a user's jobs need (the catalog's "secrets") in the same namespace.
# Environment: KYUUBI_NAMESPACE (duck-kyuubi), USER_QUOTA_PODS (20), USER_QUOTA_CPU (8 CPU of requests),
# USER_QUOTA_MEMORY (16Gi of requests), USER_QUOTA_MEMORY_LIMITS (20Gi), USER_MAX_CPU (4 per container),
# USER_MAX_MEMORY (6Gi per container).
set -euo pipefail
user=${1:?usage: add-user.sh <kyuubi user>}
# becomes part of a namespace name (DNS label); the session policy checks the same
[[ $user =~ ^[a-z]([a-z0-9-]{0,40}[a-z0-9])?$ ]] || { echo "invalid user name: $user" >&2; exit 1; }
ns=spark-$user
kyuubi_ns=${KYUUBI_NAMESPACE:-duck-kyuubi}

kubectl apply -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $ns
  labels:
    # the launcher's pods run as non-root with no capabilities, which baseline allows
    pod-security.kubernetes.io/enforce: baseline
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: kyuubi, namespace: $ns}
rules:
  - apiGroups: [""]
    resources: [pods]
    verbs: [create, get, list, delete]
  - apiGroups: [""]
    resources: [pods/log]   # STATUS and LOGS read a run's output
    verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: kyuubi, namespace: $ns}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: kyuubi}
subjects:
  - {kind: ServiceAccount, name: kyuubi, namespace: $kyuubi_ns}
---
# What one user can run at once. Launcher pods request 500m CPU and their memory limit.
apiVersion: v1
kind: ResourceQuota
metadata: {name: user, namespace: $ns}
spec:
  hard:
    pods: "${USER_QUOTA_PODS:-20}"
    requests.cpu: "${USER_QUOTA_CPU:-8}"
    requests.memory: ${USER_QUOTA_MEMORY:-16Gi}
    limits.memory: ${USER_QUOTA_MEMORY_LIMITS:-20Gi}
---
# Per container: an upper bound, and defaults for containers that set nothing.
apiVersion: v1
kind: LimitRange
metadata: {name: user, namespace: $ns}
spec:
  limits:
    - type: Container
      max: {cpu: "${USER_MAX_CPU:-4}", memory: ${USER_MAX_MEMORY:-6Gi}}
      default: {memory: 1Gi}
      defaultRequest: {cpu: 100m, memory: 256Mi}
YAML
echo "namespace $ns is ready for $user's jobs"
