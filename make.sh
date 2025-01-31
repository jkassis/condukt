#!/bin/bash

# Default CockroachDB connection string (Homebrew setup)
export DB_CONN_STRING="postgresql://root@localhost:26257/testdb?sslmode=disable"

# Function to check if CockroachDB is installed via Homebrew
check_cockroach() {
  if ! command -v cockroach &>/dev/null; then
    echo "❌ CockroachDB is not installed. Please install it with:"
    echo "   brew install cockroach"
    exit 1
  fi
}

# Setup CockroachDB (Installs, Starts, Creates DB & Table)
setupdb() {
  check_cockroach

  echo "🔧 Setting up CockroachDB..."

  # Start CockroachDB via Homebrew (if not already running)
  if brew services list | grep -q "cockroach.*started"; then
    echo "⚠️ CockroachDB is already running."
  else
    brew services start cockroach
    sleep 3
    echo "✅ CockroachDB started via Homebrew."
  fi

  # Create the test database
  cockroach sql --insecure --host=localhost:26257 -e "CREATE DATABASE IF NOT EXISTS testdb;"
  echo "✅ Database 'testdb' created."

  # Create the test table in the correct database
  cockroach sql --insecure --host=localhost:26257 -d testdb -e "
        CREATE TABLE IF NOT EXISTS test_table (
            key TEXT PRIMARY KEY,
            value INT DEFAULT 0
        );
    "
  echo "✅ Table 'test_table' created in 'testdb'."
}

# Optimize CockroachDB for benchmarking
tune() {
  check_cockroach

  echo "⚙️  Tuning CockroachDB for high concurrency..."

  cockroach sql --insecure --host=localhost:26257 -d testdb -e "
    ALTER DATABASE testdb CONFIGURE ZONE USING gc.ttlseconds = 600;
    SET CLUSTER SETTING kv.closed_timestamp.target_duration = '3s';
    SET CLUSTER SETTING sql.conn.max_lifetime = '15m';
    SET CLUSTER SETTING sql.defaults.conn_max = 5000;
    "

  echo "✅ CockroachDB tuned for performance."

  echo "🔧 Increasing macOS ulimit..."
  ulimit -n 65536
  ulimit -u 4096
  echo "✅ ulimit increased."
}

# Start CockroachDB using Homebrew
startdb() {
  check_cockroach

  echo "🚀 Starting CockroachDB..."
  brew services start cockroach
  echo "✅ CockroachDB started via Homebrew."
}

# Stop CockroachDB using Homebrew
stopdb() {
  check_cockroach

  echo "🛑 Stopping CockroachDB..."
  brew services stop cockroach
  echo "✅ CockroachDB stopped via Homebrew."
}

# Run the benchmark test using the correct connection string
bench() {
  check_cockroach

  export DB_CONN_STRING="postgresql://root@localhost:26257/testdb?sslmode=disable"
  echo "🔗 Using DB_CONN_STRING=$DB_CONN_STRING"

  echo "🛠 Running benchmark tests..."
  go test -bench . -benchtime=10s
}

# Run tests using the correct connection string
test() {
  check_cockroach

  export DB_CONN_STRING="postgresql://root@localhost:26257/testdb?sslmode=disable"
  echo "🔗 Using DB_CONN_STRING=$DB_CONN_STRING"

  echo "🛠 Running tests..."
  go test .
}

# Show usage if no command is provided
if [[ $# -eq 0 ]]; then
  echo "Usage: $0 {setupdb|tune|startdb|stopdb|test}"
  exit 1
fi

# Run the requested command
"$@"
