#!/bin/bash

# Pinterest StarRocks Migration Script
# Migrates features from pinterest-integration-3.3 to pinterest-integration-3.4.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/migration_execution.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Define commit arrays by feature area
CACHE_SELECT_COMMITS=(
    "9ba1a0842f3"  # Cache select crash fix for struct/json/map/array columns
    "d5d4498c914"  # Cache select error fix after DDL
    "1c8a63c08bd"  # CacheSelectBackendSelectorTest fix
)

CAUTHZ_COMMITS=(
    "99e7c3d23ad"  # Replace space in access type with underscore
    # Add more cauthz commits as identified
)

ROUTINE_LOAD_COMMITS=(
    "398de1428f6"  # Emit routine load lag time (CONFLICTS)
    "b2cd7d8cb24"  # Emit routine load lag time (base)
    "f05a3ab8b66"  # Bug fix
    "ca6ed81ac73"  # Emit to show routine load
)

SCHEMA_COMMITS=(
    "574127cf7f7"  # Fast schema evolution for shared-data (CONFLICTS)
    "df3d074ddaa"  # Java 11 compatibility fixes
)

BUG_FIX_COMMITS=(
    "1ca2148e597"  # HTTP result writer fix (CONFLICTS)
    "8eaab2f760e"  # Deploy serialize pool block fix
    "5fabe6d918a"  # Query hang fix for scan range delivery
)

backup_branch() {
    local branch_name="$1"
    local backup_name="${branch_name}-backup-$(date +%Y%m%d_%H%M)"
    log "Creating backup branch: $backup_name"
    git branch "$backup_name" "$branch_name"
    echo "$backup_name"
}

cherry_pick_with_retry() {
    local commit="$1"
    local description="$2"
    
    log "Attempting to cherry-pick: $commit - $description"
    
    if git cherry-pick "$commit"; then
        log "SUCCESS: Cherry-picked $commit"
        return 0
    else
        log "CONFLICT: Cherry-pick failed for $commit"
        log "Conflicts in files:"
        git diff --name-only --diff-filter=U | tee -a "$LOG_FILE"
        
        echo "Cherry-pick conflict for commit $commit"
        echo "Description: $description"
        echo "Choose action:"
        echo "1) Skip this commit"
        echo "2) Resolve manually (will pause script)"
        echo "3) Abort entire process"
        read -p "Enter choice (1-3): " choice
        
        case $choice in
            1)
                git cherry-pick --skip
                log "SKIPPED: $commit"
                return 1
                ;;
            2)
                echo "Please resolve conflicts manually, then run 'git cherry-pick --continue'"
                echo "Press Enter when done..."
                read
                log "MANUAL: Manually resolved $commit"
                return 0
                ;;
            3)
                git cherry-pick --abort
                log "ABORTED: Migration process aborted by user"
                exit 1
                ;;
        esac
    fi
}

apply_commit_group() {
    local group_name="$1"
    shift
    local commits=("$@")
    
    log "=== Applying $group_name commits ==="
    
    for commit in "${commits[@]}"; do
        cherry_pick_with_retry "$commit" "$group_name commit"
    done
}

main() {
    log "Starting Pinterest StarRocks migration script"
    log "Current branch: $(git branch --show-current)"
    
    # Verify we're on the right branch
    if [[ "$(git branch --show-current)" != "pinterest-integration-3.4.0-cherrypicks" ]]; then
        log "ERROR: Not on expected branch. Expected: pinterest-integration-3.4.0-cherrypicks"
        exit 1
    fi
    
    # Apply commits by priority/dependency order
    apply_commit_group "Cache Select" "${CACHE_SELECT_COMMITS[@]}"
    apply_commit_group "CAuthZ" "${CAUTHZ_COMMITS[@]}"
    apply_commit_group "Bug Fixes" "${BUG_FIX_COMMITS[@]}"
    apply_commit_group "Schema Changes" "${SCHEMA_COMMITS[@]}"
    apply_commit_group "Routine Load" "${ROUTINE_LOAD_COMMITS[@]}"
    
    log "Migration script completed"
    log "Review the log file: $LOG_FILE"
    log "Final branch state:"
    git log --oneline -10 | tee -a "$LOG_FILE"
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
