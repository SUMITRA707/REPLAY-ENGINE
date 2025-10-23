#!/bin/bash

# Database Snapshot Script for Juice Shop
# Creates and restores database snapshots for isolated replay testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_DIR="${SCRIPT_DIR}/snapshots"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Default values
DB_CONTAINER="juice-shop-db"
DB_NAME="juiceshop"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5432"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Database Snapshot Script for Juice Shop Replay Engine

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    create [snapshot_name]    Create a new database snapshot
    restore [snapshot_name]  Restore database from snapshot
    list                     List available snapshots
    delete [snapshot_name]   Delete a snapshot
    clean                    Clean old snapshots (older than 7 days)

Options:
    -c, --container CONTAINER    Database container name (default: $DB_CONTAINER)
    -d, --database DATABASE     Database name (default: $DB_NAME)
    -u, --user USER            Database user (default: $DB_USER)
    -p, --password PASSWORD    Database password (default: $DB_PASSWORD)
    -h, --host HOST            Database host (default: $DB_HOST)
    -P, --port PORT            Database port (default: $DB_PORT)
    --help                     Show this help message

Examples:
    $0 create baseline-snapshot
    $0 restore baseline-snapshot
    $0 list
    $0 delete old-snapshot
    $0 clean

Environment Variables:
    DB_CONTAINER    Database container name
    DB_NAME         Database name
    DB_USER         Database user
    DB_PASSWORD     Database password
    DB_HOST         Database host
    DB_PORT         Database port
EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--container)
                DB_CONTAINER="$2"
                shift 2
                ;;
            -d|--database)
                DB_NAME="$2"
                shift 2
                ;;
            -u|--user)
                DB_USER="$2"
                shift 2
                ;;
            -p|--password)
                DB_PASSWORD="$2"
                shift 2
                ;;
            -h|--host)
                DB_HOST="$2"
                shift 2
                ;;
            -P|--port)
                DB_PORT="$2"
                shift 2
                ;;
            --help)
                show_help
                exit 0
                ;;
            create|restore|list|delete|clean)
                COMMAND="$1"
                if [[ $# -gt 1 && ! "$2" =~ ^- ]]; then
                    SNAPSHOT_NAME="$2"
                    shift 2
                else
                    shift
                fi
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# Check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker is not running or not accessible"
        exit 1
    fi
}

# Check if database container exists
check_container() {
    if ! docker ps -a --format "table {{.Names}}" | grep -q "^${DB_CONTAINER}$"; then
        log_error "Database container '$DB_CONTAINER' not found"
        log_info "Available containers:"
        docker ps -a --format "table {{.Names}}\t{{.Status}}"
        exit 1
    fi
}

# Check if database container is running
check_container_running() {
    if ! docker ps --format "table {{.Names}}" | grep -q "^${DB_CONTAINER}$"; then
        log_warn "Database container '$DB_CONTAINER' is not running. Starting it..."
        docker start "$DB_CONTAINER"
        
        # Wait for container to be ready
        log_info "Waiting for database to be ready..."
        sleep 10
    fi
}

# Create snapshot directory
create_snapshot_dir() {
    mkdir -p "$SNAPSHOT_DIR"
}

# Create database snapshot
create_snapshot() {
    local snapshot_name="${SNAPSHOT_NAME:-snapshot_${TIMESTAMP}}"
    local snapshot_file="${SNAPSHOT_DIR}/${snapshot_name}.sql"
    
    log_info "Creating database snapshot: $snapshot_name"
    
    # Create snapshot using pg_dump
    if docker exec "$DB_CONTAINER" pg_dump -U "$DB_USER" -h localhost "$DB_NAME" > "$snapshot_file"; then
        log_info "Snapshot created successfully: $snapshot_file"
        
        # Create metadata file
        cat > "${snapshot_file%.sql}.meta" << EOF
{
    "snapshot_name": "$snapshot_name",
    "created_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "database": "$DB_NAME",
    "container": "$DB_CONTAINER",
    "size_bytes": $(stat -f%z "$snapshot_file" 2>/dev/null || stat -c%s "$snapshot_file" 2>/dev/null || echo 0)
}
EOF
        
        log_info "Snapshot metadata created: ${snapshot_file%.sql}.meta"
    else
        log_error "Failed to create snapshot"
        exit 1
    fi
}

# Restore database from snapshot
restore_snapshot() {
    local snapshot_name="${SNAPSHOT_NAME}"
    local snapshot_file="${SNAPSHOT_DIR}/${snapshot_name}.sql"
    
    if [[ -z "$snapshot_name" ]]; then
        log_error "Snapshot name is required for restore operation"
        exit 1
    fi
    
    if [[ ! -f "$snapshot_file" ]]; then
        log_error "Snapshot file not found: $snapshot_file"
        log_info "Available snapshots:"
        list_snapshots
        exit 1
    fi
    
    log_warn "This will replace the current database content!"
    read -p "Are you sure you want to continue? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Restore cancelled"
        exit 0
    fi
    
    log_info "Restoring database from snapshot: $snapshot_name"
    
    # Drop and recreate database
    docker exec "$DB_CONTAINER" psql -U "$DB_USER" -h localhost -c "DROP DATABASE IF EXISTS ${DB_NAME}_temp;"
    docker exec "$DB_CONTAINER" psql -U "$DB_USER" -h localhost -c "CREATE DATABASE ${DB_NAME}_temp;"
    
    # Restore from snapshot
    if docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -h localhost "${DB_NAME}_temp" < "$snapshot_file"; then
        # Swap databases
        docker exec "$DB_CONTAINER" psql -U "$DB_USER" -h localhost -c "DROP DATABASE IF EXISTS ${DB_NAME}_backup;"
        docker exec "$DB_CONTAINER" psql -U "$DB_USER" -h localhost -c "ALTER DATABASE $DB_NAME RENAME TO ${DB_NAME}_backup;"
        docker exec "$DB_CONTAINER" psql -U "$DB_USER" -h localhost -c "ALTER DATABASE ${DB_NAME}_temp RENAME TO $DB_NAME;"
        
        log_info "Database restored successfully from snapshot: $snapshot_name"
        log_info "Previous database backed up as: ${DB_NAME}_backup"
    else
        log_error "Failed to restore snapshot"
        # Clean up temp database
        docker exec "$DB_CONTAINER" psql -U "$DB_USER" -h localhost -c "DROP DATABASE IF EXISTS ${DB_NAME}_temp;"
        exit 1
    fi
}

# List available snapshots
list_snapshots() {
    log_info "Available snapshots:"
    
    if [[ ! -d "$SNAPSHOT_DIR" ]] || [[ -z "$(ls -A "$SNAPSHOT_DIR" 2>/dev/null)" ]]; then
        log_info "No snapshots found"
        return
    fi
    
    printf "%-30s %-20s %-15s %s\n" "NAME" "CREATED" "SIZE" "DATABASE"
    printf "%-30s %-20s %-15s %s\n" "----" "-------" "----" "--------"
    
    for snapshot_file in "$SNAPSHOT_DIR"/*.sql; do
        if [[ -f "$snapshot_file" ]]; then
            snapshot_name=$(basename "$snapshot_file" .sql)
            meta_file="${snapshot_file%.sql}.meta"
            
            if [[ -f "$meta_file" ]]; then
                created_at=$(jq -r '.created_at' "$meta_file" 2>/dev/null || echo "Unknown")
                size_bytes=$(jq -r '.size_bytes' "$meta_file" 2>/dev/null || echo "0")
                database=$(jq -r '.database' "$meta_file" 2>/dev/null || echo "Unknown")
                
                # Format size
                if [[ "$size_bytes" -gt 1048576 ]]; then
                    size_display=$(echo "scale=1; $size_bytes/1048576" | bc -l 2>/dev/null || echo "?")
                    size_display="${size_display}MB"
                elif [[ "$size_bytes" -gt 1024 ]]; then
                    size_display=$(echo "scale=1; $size_bytes/1024" | bc -l 2>/dev/null || echo "?")
                    size_display="${size_display}KB"
                else
                    size_display="${size_bytes}B"
                fi
                
                printf "%-30s %-20s %-15s %s\n" "$snapshot_name" "$created_at" "$size_display" "$database"
            else
                printf "%-30s %-20s %-15s %s\n" "$snapshot_name" "Unknown" "Unknown" "Unknown"
            fi
        fi
    done
}

# Delete snapshot
delete_snapshot() {
    local snapshot_name="${SNAPSHOT_NAME}"
    
    if [[ -z "$snapshot_name" ]]; then
        log_error "Snapshot name is required for delete operation"
        exit 1
    fi
    
    local snapshot_file="${SNAPSHOT_DIR}/${snapshot_name}.sql"
    local meta_file="${SNAPSHOT_DIR}/${snapshot_name}.meta"
    
    if [[ ! -f "$snapshot_file" ]]; then
        log_error "Snapshot not found: $snapshot_name"
        exit 1
    fi
    
    log_warn "This will permanently delete the snapshot: $snapshot_name"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f "$snapshot_file" "$meta_file"
        log_info "Snapshot deleted: $snapshot_name"
    else
        log_info "Delete cancelled"
    fi
}

# Clean old snapshots
clean_snapshots() {
    local days="${1:-7}"
    local cutoff_date=$(date -d "$days days ago" +"%Y-%m-%d" 2>/dev/null || date -v-${days}d +"%Y-%m-%d" 2>/dev/null || echo "")
    
    if [[ -z "$cutoff_date" ]]; then
        log_error "Failed to calculate cutoff date"
        exit 1
    fi
    
    log_info "Cleaning snapshots older than $days days (before $cutoff_date)"
    
    local cleaned_count=0
    for meta_file in "$SNAPSHOT_DIR"/*.meta; do
        if [[ -f "$meta_file" ]]; then
            created_at=$(jq -r '.created_at' "$meta_file" 2>/dev/null)
            snapshot_name=$(jq -r '.snapshot_name' "$meta_file" 2>/dev/null)
            
            if [[ "$created_at" < "$cutoff_date" ]]; then
                snapshot_file="${meta_file%.meta}.sql"
                rm -f "$snapshot_file" "$meta_file"
                log_info "Cleaned old snapshot: $snapshot_name"
                ((cleaned_count++))
            fi
        fi
    done
    
    log_info "Cleaned $cleaned_count old snapshots"
}

# Main function
main() {
    # Override with environment variables if set
    DB_CONTAINER="${DB_CONTAINER:-$DB_CONTAINER}"
    DB_NAME="${DB_NAME:-$DB_NAME}"
    DB_USER="${DB_USER:-$DB_USER}"
    DB_PASSWORD="${DB_PASSWORD:-$DB_PASSWORD}"
    DB_HOST="${DB_HOST:-$DB_HOST}"
    DB_PORT="${DB_PORT:-$DB_PORT}"
    
    # Parse command line arguments
    parse_args "$@"
    
    # Validate command
    if [[ -z "$COMMAND" ]]; then
        log_error "Command is required"
        show_help
        exit 1
    fi
    
    # Check prerequisites
    check_docker
    check_container
    
    # Create snapshot directory
    create_snapshot_dir
    
    # Execute command
    case "$COMMAND" in
        create)
            check_container_running
            create_snapshot
            ;;
        restore)
            check_container_running
            restore_snapshot
            ;;
        list)
            list_snapshots
            ;;
        delete)
            delete_snapshot
            ;;
        clean)
            clean_snapshots
            ;;
        *)
            log_error "Unknown command: $COMMAND"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"