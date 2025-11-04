#!/bin/bash
set -e

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Installing system dependencies for Playwright..."
playwright install-deps chromium

echo "Installing Playwright browsers..."
playwright install chromium

echo "Build completed successfully!"