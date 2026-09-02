#!/bin/bash

##############################################################
# This script is used to create an internal tarball for StarRocks
# Usage: 
#    sh make_starrocks_oss_tarball.sh --help
# Eg:
#    sh make_starrocks_oss_tarball.sh  4.1.0-rc01                                   build all
#    sh make_starrocks_oss_tarball.sh 4.1.0-rc01 --skip-oss-build                   use existing oss build
#    sh make_starrocks_oss_tarball.sh 4.1.0-rc01 --source-build-dir /home/$USER/code/starrocks/output  use custom build directory
#    sh make_starrocks_oss_tarball.sh 4.1.0-rc01 --use-existing-container <name>    use existing docker container
#
# This script will leverage an all-in-on docker development environment   
# image that contains all dependencies required to build StarRocks. It will 
# build the StarRocks oss and starrocks-internal jar files and merge them
# into a tarball.
##############################################################

# Exit on error
set -e

# Check args
usage() {
  echo "
Usage: $0 <version> <options>
  Required options:
      version                                StarRocks version to build
  Optional options:
      --skip-oss-build                       use existing oss build - looks for build in source build directory
      --source-build-dir <path>              use build from specified directory instead of pinterest-starrocks-oss/output
      --enable-debug-symbols                 Keep debug symbols in the build
      --use-existing-container <name>        use existing docker container
      --docker-container-version <version>   Docker image version to use
      --vanilla                              Build vanilla StarRocks without Pinterest customizations (skip starrocks-internal)
      --no-cleanup                           Do not cleanup the docker container after build
   Eg.
     $0 4.1.0-rc01                                                        build all
     $0 4.1.0-rc01 --skip-oss-build                                       use existing oss build
     $0 4.1.0-rc01 --source-build-dir /home/\$USER/code/starrocks/output   use custom build directory
     $0 4.1.0-rc01 --use-existing-container starrocks-internal-build      use existing docker container
     $0 4.1.0-rc01 --docker-container-version 4.1-latest                  use specific docker image version
     $0 4.1.0-rc01 --no-cleanup                                            do not cleanup docker container after build
  "
  exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

VERSION=$1
shift

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'skip-oss-build,source-build-dir:,enable-debug-symbols,use-existing-container:,docker-container-version:,vanilla,no-cleanup' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

SKIP_OSS_BUILD=0
SOURCE_BUILD_DIR="pinterest-starrocks-oss/output"
ENABLE_DEBUG_SYMBOLS=0
USE_EXISTING_CONTAINER=""
VANILLA_BUILD=0
NO_CLEANUP=0
while true; do
    case "$1" in
        --skip-oss-build) SKIP_OSS_BUILD=1; shift ;;
        --source-build-dir) SOURCE_BUILD_DIR=$2 ; shift 2 ;;
        --enable-debug-symbols) ENABLE_DEBUG_SYMBOLS=1; shift ;;
        --use-existing-container) USE_EXISTING_CONTAINER=$2 ; shift 2 ;;
        --docker-container-version) DOCKER_VERSION=$2 ; shift 2 ;;
        --vanilla) VANILLA_BUILD=1; shift ;;
        --no-cleanup) NO_CLEANUP=1; shift ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

# Expand variables/tildes in build directory path  
SOURCE_BUILD_DIR=$(eval echo "$SOURCE_BUILD_DIR")

# Validate source build directory structure
validate_build_directory() {
    local build_dir="$1"
    
    echo "Validating build directory structure at: $build_dir"
    
    # Check if directory exists
    if [ ! -d "$build_dir" ]; then
        echo "ERROR: Build directory does not exist: $build_dir"
        exit 1
    fi
    
    # Check for fe directory and essential files
    if [ ! -d "$build_dir/fe" ]; then
        echo "ERROR: Frontend directory not found at: $build_dir/fe"
        exit 1
    fi
    
    if [ ! -d "$build_dir/fe/lib" ]; then
        echo "ERROR: Frontend lib directory not found at: $build_dir/fe/lib"
        exit 1
    fi
    
    if [ ! -f "$build_dir/fe/bin/start_fe.sh" ]; then
        echo "ERROR: Frontend startup script not found at: $build_dir/fe/bin/start_fe.sh"
        exit 1
    fi
    
    if [ ! -f "$build_dir/fe/lib/starrocks-fe.jar" ]; then
        echo "ERROR: StarRocks frontend JAR not found at: $build_dir/fe/lib/starrocks-fe.jar"
        exit 1
    fi
    
    # Check for be directory and essential files
    if [ ! -d "$build_dir/be" ]; then
        echo "ERROR: Backend directory not found at: $build_dir/be"
        exit 1
    fi
    
    if [ ! -d "$build_dir/be/lib" ]; then
        echo "ERROR: Backend lib directory not found at: $build_dir/be/lib"
        exit 1
    fi
    
    if [ ! -f "$build_dir/be/lib/starrocks_be" ]; then
        echo "ERROR: Backend binary not found at: $build_dir/be/lib/starrocks_be"
        exit 1
    fi
    
    echo "✓ Build directory validation passed"
}

# Normalize FE artifact filenames so starrocks-internal can resolve them via
# the stable 4.1.0 systemPath declared in pom.xml.
# Written in POSIX sh-compatible syntax (no bash arrays).
normalize_fe_artifact_names() {
    local build_dir="$1"
    local fe_lib_dir="$build_dir/fe/lib"
    local expected_version="4.1.0"

    local fe_core_actual
    local fe_spi_actual
    fe_core_actual=$(ls "$fe_lib_dir"/fe-core-*.jar 2>/dev/null | head -1)
    fe_spi_actual=$(ls "$fe_lib_dir"/fe-spi-*.jar 2>/dev/null | head -1)

    if [ -z "$fe_core_actual" ]; then
        echo "ERROR: no fe-core jar found in $fe_lib_dir"
        ls "$fe_lib_dir"/ || true
        exit 1
    fi
    if [ -z "$fe_spi_actual" ]; then
        echo "ERROR: no fe-spi jar found in $fe_lib_dir"
        ls "$fe_lib_dir"/ || true
        exit 1
    fi

    local fe_core_expected="$fe_lib_dir/fe-core-${expected_version}.jar"
    local fe_spi_expected="$fe_lib_dir/fe-spi-${expected_version}.jar"

    if [ "$fe_core_actual" != "$fe_core_expected" ]; then
        sudo ln -sf "$(basename "$fe_core_actual")" "$fe_core_expected"
        echo "Linked $(basename "$fe_core_actual") -> $(basename "$fe_core_expected")"
    else
        echo "fe-core already at expected name: $(basename "$fe_core_expected")"
    fi
    if [ "$fe_spi_actual" != "$fe_spi_expected" ]; then
        sudo ln -sf "$(basename "$fe_spi_actual")" "$fe_spi_expected"
        echo "Linked $(basename "$fe_spi_actual") -> $(basename "$fe_spi_expected")"
    else
        echo "fe-spi already at expected name: $(basename "$fe_spi_expected")"
    fi
}

# Clean up the StarRocks all-in-one docker container
cleanup() {
    if [ $NO_CLEANUP -eq 1 ]; then
        echo "Skipping cleanup (--no-cleanup flag set)"
        return
    fi
    if [ -z "$USE_EXISTING_CONTAINER" ]; then
        echo "Cleaning up the StarRocks all-in-one docker container"
        if [ "$(docker ps -a | grep starrocks-internal-build)" ]; then
            docker stop starrocks-internal-build
            docker rm starrocks-internal-build
        fi
    fi
    echo "Cleanup completed"
}

# Set trap to call cleanup function on EXIT
trap cleanup EXIT

cd ~/code/starrocks-pinterest-configurations
# Verify the pinterest-starrocks-oss submodule present
if [ "$(ls pinterest-starrocks-oss)" ]; then
    echo "pinterest-starrocks-oss submodule present"
else
    echo "pinterest-starrocks-oss submodule not present"
    # if git repo clone the pinterest-starrocks-oss submodule
    if [ "$(ls -a .git)" ]; then
        git submodule update --init --recursive
    else
        echo "Not a git repository"
        echo "Please clone and sync the pinterest-starrocks-oss submodule"
        exit 1
    fi
fi

# Validate build directory if using custom dir or skipping build
if [ "$SOURCE_BUILD_DIR" != "pinterest-starrocks-oss/output" ] || [ $SKIP_OSS_BUILD -eq 1 ]; then
    validate_build_directory "$SOURCE_BUILD_DIR"
fi

# Pull and enter the StarRocks all-in-one docker container
if [ -z "$USE_EXISTING_CONTAINER" ]; then
    # If docker version is not provided, use the same version as the StarRocks version
    if [ -z "$DOCKER_VERSION" ]; then
        DOCKER_VERSION=$VERSION
    fi
    echo "Pulling the StarRocks all-in-one docker container"
    docker pull starrocks/dev-env-ubuntu:${DOCKER_VERSION}
    docker run -it -v ~/.m2:/root/.m2 -v ~/code/starrocks-pinterest-configurations:/root/starrocks-pinterest-configurations --name starrocks-internal-build -d starrocks/dev-env-ubuntu:${DOCKER_VERSION}
else
    if [ "$(docker ps -a | grep $USE_EXISTING_CONTAINER)" ]; then
        echo "Using existing docker container: $USE_EXISTING_CONTAINER"
    else
        echo "Docker container $USE_EXISTING_CONTAINER not found"
        exit 1
    fi
fi

# Setup thirdparty directory to avoid async-profiler error
if [ $SKIP_OSS_BUILD -eq 0 ] && [ "$SOURCE_BUILD_DIR" == "pinterest-starrocks-oss/output" ]; then
    echo "Setting up thirdparty directory to avoid async-profiler error"
    docker exec ${USE_EXISTING_CONTAINER:-starrocks-internal-build} /bin/bash -c "mkdir -p /var/local/thirdparty/installed/async-profiler"
    echo "Created thirdparty directory: /var/local/thirdparty/installed"
    
    # Fix Ubuntu/CentOS library path incompatibility (only if needed)
    # StarRocks build expects /usr/lib64/libz.so but Ubuntu only has libz.so.1 (no -dev package)
    docker exec ${USE_EXISTING_CONTAINER:-starrocks-internal-build} /bin/bash -c "
        if [ ! -e /usr/lib64/libz.so ] && [ -e /usr/lib/x86_64-linux-gnu/libz.so.1 ]; then
            echo 'Setting up libz.so symlink for Ubuntu compatibility'
            ln -sf /usr/lib/x86_64-linux-gnu/libz.so.1 /usr/lib64/libz.so
        fi
    "
fi

# Use existing build or build from scratch
if [ $SKIP_OSS_BUILD -eq 1 ] || [ "$SOURCE_BUILD_DIR" != "pinterest-starrocks-oss/output" ]; then
    echo "Using existing oss build from $SOURCE_BUILD_DIR"
else
    echo "Building StarRocks oss"
    BUILD_PARALLEL=${PARALLEL:-2}
    echo "Using PARALLEL=$BUILD_PARALLEL for build"
    echo "Setting STARROCKS_VERSION=$VERSION"
    docker exec ${USE_EXISTING_CONTAINER:-starrocks-internal-build} /bin/bash -c "cd /root/starrocks-pinterest-configurations/pinterest-starrocks-oss && export STARROCKS_VERSION=$VERSION && ./build.sh --fe --be --enable-shared-data -j $BUILD_PARALLEL"
    echo "StarRocks oss build completed"
fi

# Ensure the OSS FE jars match the stable names expected by starrocks-internal.
normalize_fe_artifact_names "$SOURCE_BUILD_DIR"

# Build the starrocks-internal jar file (skip if vanilla build)
if [ $VANILLA_BUILD -eq 0 ]; then
    docker exec ${USE_EXISTING_CONTAINER:-starrocks-internal-build} /bin/bash -c "cd /root/starrocks-pinterest-configurations/starrocks-internal && mvn clean install"

    # Merge the fe and starrocks-internal jar files
    sudo cp starrocks-internal/target/starrocks-internal-4.1.0.jar $SOURCE_BUILD_DIR/fe/lib/starrocks-internal-4.1.0.jar
else
    echo "Vanilla build: skipping starrocks-internal build and jar merge"
fi

# Remove the existing bdb jar file from the fe lib directory
# if [ "$(ls -a $SOURCE_BUILD_DIR/fe/lib/starrocks-bdb-je-*.jar)" ]; then
#     sudo rm $SOURCE_BUILD_DIR/fe/lib/starrocks-bdb-je-*.jar
# fi

if [ $ENABLE_DEBUG_SYMBOLS -eq 1 ]; then
    if [ -z "$(ls -a $SOURCE_BUILD_DIR/be/lib/starrocks_be.debuginfo)" ]; then
        echo "enable-debug-symbols flag set but starrocks_be.debuginfo not found at $SOURCE_BUILD_DIR/be/lib"
        exit 1
    fi
    echo "Keeping be/lib/starrocks_be.debuginfo"
    VERSION="$VERSION-debug"
else
    echo "Removing be/lib/starrocks_be.debuginfo"
    # Remove large non-essential debug files from backend lib directory
    if [ "$(ls -a $SOURCE_BUILD_DIR/be/lib/starrocks_be.debuginfo)" ]; then
        sudo rm $SOURCE_BUILD_DIR/be/lib/starrocks_be.debuginfo
    fi
fi

# Copy a recompiled bdb jar file to the fe lib directory
# This is a temporary fix to address the leader election issue present in the default bdb jar file
# sudo aws s3 cp s3://datausers/jenkins/starrocks/StarRocks-3.3/starrocks-bdb-je-18.3.18.jar $SOURCE_BUILD_DIR/fe/lib/starrocks-bdb-je-18.3.18.jar

# Create a tarball from the source build directory
sudo rm -rf StarRocks-$VERSION
sudo mkdir StarRocks-$VERSION
sudo cp -r $SOURCE_BUILD_DIR/* StarRocks-$VERSION/
sudo tar -czvf StarRocks-$VERSION.tar.gz StarRocks-$VERSION/
sudo rm -rf StarRocks-$VERSION
echo "StarRocks-$VERSION.tar.gz created successfully"
