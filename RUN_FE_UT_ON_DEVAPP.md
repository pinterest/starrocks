# Running StarRocks FE Unit Tests on devapp EC2

This guide covers how to run StarRocks frontend (FE) unit tests on the devapp Ubuntu EC2 node instead of locally.

## Why Run on devapp?

- **Consistency**: Ubuntu standardized environment (vs. local macOS with JDK version mismatches)
- **No local resource impact**: Leave your laptop free for other work
- **Reproducibility**: Clean build environment each run
- **CI-ready**: Matches production test environment

## Prerequisites

### Local machine
- SSH key configured for devapp access
- Basic terminal/shell access

### devapp EC2 node
- Ubuntu 20.04+ (LTS recommended)
- OpenJDK 11 or 17
- Maven 3.6+
- Python 3
- GCC 5.3.1+

## Quick Start

### 1. SSH into devapp

```bash
ssh ec2-user@devapp.internal
# or
ssh -i /path/to/key.pem ubuntu@devapp-instance-ip
```

### 2. Clone or sync the repo

```bash
cd /home/ubuntu/code
git clone https://github.com/StarRocks/starrocks.git
cd starrocks
```

Or if already cloned, sync latest changes:

```bash
cd ~/code/starrocks
git pull origin main
```

### 3. Set up environment on devapp (if not already done)

```bash
cd ~/code/starrocks

# Check Java version
java -version

# Verify Maven
mvn --version

# Verify Python 3
python3 --version

# Export STARROCKS_HOME
export STARROCKS_HOME=$PWD

# Source the repo environment
. ./env.sh
```

### 4. Run FE unit tests

Run all tests:

```bash
./run-fe-ut.sh -j4
```

Run specific test class:

```bash
./run-fe-ut.sh --test com.starrocks.utframe.Demo
```

Run with coverage:

```bash
./run-fe-ut.sh --coverage -j4
```

Dry-run (validate compilation only):

```bash
./run-fe-ut.sh --dry-run -j4
```

### 5. Collect results

Test results are in:

```bash
cd ~/code/starrocks/fe/fe-core
# Test reports
cat target/surefire-reports/TEST-*.xml
# Or summary
ls -la target/surefire-reports/
```

To copy logs back to local machine:

```bash
# From your local machine
scp -r ec2-user@devapp:/home/ubuntu/code/starrocks/fe/fe-core/target/surefire-reports ~/Downloads/fe-ut-results-$(date +%s)
```

## Environment Setup on devapp (First Time Only)

### Install Java 11 or 17

#### Ubuntu with apt
```bash
sudo apt update
sudo apt install openjdk-17-jdk
# or
sudo apt install openjdk-11-jdk

# Verify
java -version
```

#### Using SDKMAN (if multiple JDKs needed)
```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 17.0.8-tem
```

### Install Maven

```bash
sudo apt install maven
# or download manually
wget https://archive.apache.org/dist/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz
tar xzf apache-maven-3.9.11-bin.tar.gz
export PATH=$PWD/apache-maven-3.9.11/bin:$PATH
```

### Install other dependencies

```bash
sudo apt install python3 python3-pip gcc g++ cmake ninja-build
```

## Running Tests Remotely via SSH (One-liner)

From your **local machine**, run tests on devapp without interactive SSH:

```bash
ssh ec2-user@devapp << 'EOF'
cd ~/code/starrocks
export STARROCKS_HOME=$PWD
. ./env.sh
./run-fe-ut.sh -j4 2>&1 | tee /tmp/fe-ut-run-$(date +%Y%m%d-%H%M%S).log
EOF
```

Then retrieve the log:

```bash
scp ec2-user@devapp:/tmp/fe-ut-run-*.log ~/Downloads/
```

## Parallel & Memory Tuning

### Run with more parallel jobs (if devapp is powerful enough)

```bash
./run-fe-ut.sh -j8  # 8 parallel threads
```

### Increase Maven/Java heap for large tests

```bash
export MAVEN_OPTS="-Xmx4g -Xms4g"
./run-fe-ut.sh -j4
```

## Troubleshooting

### Maven/Java not found

```bash
# On devapp, ensure env vars are set
export JAVA_HOME=$(which java | xargs dirname | xargs dirname)
export PATH=$JAVA_HOME/bin:$PATH

# Then run
./run-fe-ut.sh -j4
```

### Out of disk space

Check available space:

```bash
df -h
# Clean old builds
cd ~/code/starrocks
rm -rf fe/fe-core/target
rm -rf be/build
```

### Build fails with version mismatch

Rebuild clean:

```bash
cd ~/code/starrocks
./run-fe-ut.sh --dry-run  # validate setup first
# Then actual run
./run-fe-ut.sh -j4
```

### Connection drops during long test runs

Use `tmux` or `screen` to run in a persistent session:

```bash
ssh ec2-user@devapp
tmux new-session -s fe-ut
cd ~/code/starrocks && export STARROCKS_HOME=$PWD && . ./env.sh
./run-fe-ut.sh -j4
# Detach with Ctrl+B then D
# Later: tmux attach -t fe-ut
```

## Example: Full workflow

```bash
# 1. SSH to devapp
ssh ec2-user@devapp

# 2. Inside devapp, update repo and run tests
cd ~/code/starrocks
git pull origin main
export STARROCKS_HOME=$PWD
. ./env.sh

# 3. Run tests
./run-fe-ut.sh -j4

# 4. In another terminal on your local machine, copy results
scp -r ec2-user@devapp:~/code/starrocks/fe/fe-core/target/surefire-reports ./results
```

## Notes

- First run may take longer due to Maven downloading dependencies
- Subsequent runs are faster (dependencies cached)
- Test results persist in `fe/fe-core/target/surefire-reports` for 24 hours by default
- For CI integration, consider creating a devapp user with key-based SSH and no password prompt

## See Also

- `./run-fe-ut.sh --help` for all test runner options
- `env.sh` for environment variable documentation
- `custom_env.sh` for repo-specific overrides (created locally for macOS)
