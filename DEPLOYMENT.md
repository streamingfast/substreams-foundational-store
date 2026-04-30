# Badger-Backed Stores Deployment Guide

## Overview

This deployment sets up a test environment for the Badger-backed foundational store write path. It allows testing the gRPC API (SetAll, FlushUpToBlock, EvictUpToBlock) with persistent Badger storage.

## Architecture

```
┌─────────────────┐
│  Test Client    │
│  (CLI commands) │
└────────┬────────┘
         │ gRPC (port 9020)
         ▼
┌─────────────────────────┐
│  badger-store-dev       │
│  StatefulSet (1 replica)│
│  ┌────────────────────┐ │
│  │ foundational-store │ │
│  │ gRPC Server        │ │
│  └─────────┬──────────┘ │
│            │            │
│  ┌─────────▼──────────┐ │
│  │  Badger Storage    │ │
│  │  (50Gi PVC)        │ │
│  └────────────────────┘ │
└─────────────────────────┘
```

## What Was Created

### In `substreams-foundational-store` repo:

**CLI Commands:**
- `cmd/foundational-store/set.go` - SetAll gRPC wrapper
- `cmd/foundational-store/flush.go` - FlushUpToBlock gRPC wrapper
- `cmd/foundational-store/evict.go` - EvictUpToBlock gRPC wrapper
- `cmd/foundational-store/main.go` - Updated with new commands

**Test Script:**
- `test-badger-store.sh` - 7 test scenarios covering all operations

### In `sf-operator` repo:

**Library (`lib/badger-store-dev/`):**
- `config.libsonnet` - Configuration defaults
- `factories.libsonnet` - StatefulSet, Service, ServiceAccount factories
- `badger_store_dev.libsonnet` - Main library composition

**Environment (`environments/badger-store-dev/`):**
- `spec.json` - Tanka environment spec (namespace: `badger-store-dev`)
- `main.jsonnet` - Environment configuration

## Deployment Steps

### 1. Build and Push Docker Image

```bash
cd /Users/ulyssecorbeil/Documents/SF/substreams-foundational-store

# Build the binary
go build -o foundational-store ./cmd/foundational-store

# Build Docker image with commit hash
docker build -t ghcr.io/streamingfast/substreams-foundational-store:510646a .

# Push to registry (requires authentication)
docker push ghcr.io/streamingfast/substreams-foundational-store:510646a
```

### 2. Verify Manifests

```bash
cd /Users/ulyssecorbeil/Documents/SF/sf-operator

# Preview what will be deployed (NO APPLY)
TANKA_DANGEROUS_ALLOW_REDIRECT=true tk show environments/badger-store-dev
```

### 3. Deploy to GKE

```bash
# Deploy to saas-us-central1 cluster
tk apply environments/badger-store-dev
```

This creates:
- **Namespace**: `badger-store-dev`
- **StatefulSet**: `badger-store-dev` (1 replica)
- **Services**: 
  - `badger-store-dev` (headless for StatefulSet)
  - `badger-store-dev-public` (ClusterIP for external access)
- **PVC**: `datadir-badger-store-dev-0` (50Gi, standard-rwo)
- **ServiceAccount**: `badger-store-dev`

### 4. Verify Deployment

```bash
# Check pod status
kubectl get pods -n badger-store-dev

# Check logs
kubectl logs -n badger-store-dev badger-store-dev-0 -f

# Check PVC
kubectl get pvc -n badger-store-dev
```

### 5. Port Forward for Testing

```bash
# Forward gRPC port to localhost
kubectl port-forward -n badger-store-dev svc/badger-store-dev-public 9020:9020
```

### 6. Run Tests

In another terminal:

```bash
cd /Users/ulyssecorbeil/Documents/SF/substreams-foundational-store

# Make test script executable
chmod +x test-badger-store.sh

# Run all test scenarios
./test-badger-store.sh
```

## Test Scenarios

The test script covers:

1. **Basic SET/GET** - Simple key-value storage
2. **ADD Policy** - Accumulation (10 + 5 = 15)
3. **MIN Policy** - Keep minimum value
4. **MAX Policy** - Keep maximum value
5. **SET_IF_NOT_EXISTS** - Write-once semantics
6. **FlushUpToBlock** - Persist cache to Badger
7. **EvictUpToBlock** - Fork handling (reorg)

## Manual Testing Examples

```bash
# Set a value
foundational-store set \
  --server=localhost:9020 \
  --key=mykey \
  --value=myvalue \
  --update-policy=SET \
  --block-number=100 \
  --value-type=bytes

# Get a value
foundational-store get \
  --server=localhost:9020 \
  --key=mykey \
  --block-number=100 \
  --encoding=hex

# Flush cache to Badger
foundational-store flush \
  --server=localhost:9020 \
  --block-number=100

# Evict blocks (simulate reorg)
foundational-store evict \
  --server=localhost:9020 \
  --block-number=95
```

## Updating the Deployment

### Update Image Version

1. Make code changes
2. Commit changes (get new commit hash)
3. Update `main.jsonnet`:

```jsonnet
_images+:: {
  badger_store: 'ghcr.io/streamingfast/substreams-foundational-store:<NEW_COMMIT_HASH>',
},
```

4. Build and push new image
5. Redeploy:

```bash
tk apply environments/badger-store-dev
```

### Update Configuration

Edit `environments/badger-store-dev/main.jsonnet`:

```jsonnet
_config+:: {
  badger_store+: {
    replicas: 2,  // Scale up
    log_level: 'debug',  // More verbose logging
    resources+: {
      requests: ['1', '2Gi'],  // Increase resources
    },
  },
},
```

Then redeploy with `tk apply`.

## Cleanup

```bash
# Delete the entire environment
tk prune environments/badger-store-dev

# Or manually delete
kubectl delete namespace badger-store-dev
```

## Troubleshooting

### Pod won't start

```bash
kubectl describe pod -n badger-store-dev badger-store-dev-0
kubectl logs -n badger-store-dev badger-store-dev-0
```

### PVC issues

```bash
kubectl get events -n badger-store-dev
kubectl describe pvc -n badger-store-dev datadir-badger-store-dev-0
```

### Image pull errors

```bash
# Check if image exists
docker pull ghcr.io/streamingfast/substreams-foundational-store:510646a

# Check imagePullSecrets if private registry
kubectl get serviceaccount -n badger-store-dev badger-store-dev -o yaml
```

### gRPC connection issues

```bash
# Test connectivity from within cluster
kubectl run -it --rm debug --image=alpine --restart=Never -n badger-store-dev -- sh
apk add grpc
grpcurl -plaintext badger-store-dev-public:9020 list

# Test from localhost (ensure port-forward is running)
grpcurl -plaintext localhost:9020 list
```

## Architecture Notes

- **StatefulSet** used for stable network identity and persistent storage
- **Headless service** (`clusterIP: None`) for StatefulSet DNS
- **Public service** (ClusterIP) for external/test access
- **No HPA** - single replica for testing
- **No resource limits** on storage - fixed 50Gi PVC
- **standard-rwo** StorageClass for GKE persistent disks

## Next Steps

After successful testing:

1. Integrate with Tier2 Substreams execution
2. Add performance benchmarking
3. Test fork handling with real blockchain data
4. Consider PostgreSQL backend alternative
5. Deploy foundational-stores for production chains
