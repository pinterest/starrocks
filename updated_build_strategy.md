# Updated Build & Validation Strategy

## Docker Environment Strategy

Based on your build process, we need to consider StarOS/StarMgr dependency compatibility:

### Docker Image Selection for 3.4.0 Build:
- **For current 3.4.0 cherry-picks**: Start with `starrocks/dev-env-centos7:3.4-latest` (if available)
- **Fallback**: Use `starrocks/dev-env-centos7:latest` and monitor for dependency issues
- **Key concern**: RIG commits heavily interact with StarOS (`com.staros.proto.*` imports)

### Build Process Integration:

**Phase 1-3 Validation Process:**
```bash
# After each phase of cherry-picks
docker run -it \
  -v /home/cbrennan/code/starrocks/.m2:/root/.m2 \
  -v /home/cbrennan/code/starrocks:/root/starrocks \
  --name branch-3.4-migration \
  -d starrocks/dev-env-centos7:3.4-latest

docker exec -it branch-3.4-migration /bin/bash
cd /root/starrocks
git checkout pinterest-integration-3.4.0-cherrypicks

# Build with shared-data (critical for RIG functionality)
./build.sh --clean --fe --be --enable-shared-data

# Run targeted tests after each phase
./run-fe-ut.sh -P com.starrocks.privilege.cauthz  # After Phase 1
./run-fe-ut.sh -P com.starrocks.system.WorkerGroupManagerTest  # After Phase 2
./run-fe-ut.sh -P com.starrocks.qe.CacheSelectBackendSelectorTest  # After Phase 3
```

**Dependency Compatibility Check:**
From commit `733b7282580`, RIG uses:
- `com.staros.proto.WorkerGroupDetailInfo`
- `com.staros.proto.WorkerGroupSpec` 
- StarOS worker group management APIs

**Risk Assessment:**
- **Medium Risk**: RIG commits may require newer StarOS version in docker image
- **Mitigation**: Test build after Phase 2 (RIG implementation) to catch dependency issues early

### BDB-JE Library Handling:
Following your process:
```bash
# Remove bad version and copy good version
rm /root/starrocks/output/fe/lib/starrocks-bdb-je-18.3.19.jar
docker cp ~/Downloads/starrocks-bdb-je-18.3.18.jar branch-3.4-migration:/root/starrocks/output/fe/lib/
```

## Updated Migration Execution Plan

**Pre-Phase Setup:**
1. Determine correct 3.4 docker image
2. Set up docker container for build/test cycles
3. Validate base 3.4.0 builds successfully with `--enable-shared-data`

**Phase Execution with Docker Validation:**
- After Phase 1 (CAuthZ): Quick compile check
- After Phase 2 (RIG): **Full build + test** (highest dependency risk)  
- After Phase 3 (Cache+RIG): Full build + integration test
- After Phase 4: Final validation build

**Dependency Issue Handling:**
If we encounter StarOS/StarMgr compatibility issues:
1. **Check for newer 3.4 docker image** with updated dependencies
2. **Surface to you** for guidance on dependency version compatibility
3. **Consider partial cherry-pick** of RIG components if needed
