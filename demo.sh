#!/bin/bash
echo "=== DEMO ==="
# 1. Benchmark (Chạy trên WSL cực kỳ ổn định)
mvn compile exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath com.student.Main"

