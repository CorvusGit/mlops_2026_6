#!/bin/bash
set -e

CONDA_BIN="/opt/conda/bin/conda"
PIP_BIN="/opt/conda/bin/pip"

$PIP_BIN install --upgrade pip
$PIP_BIN install mlflow seaborn