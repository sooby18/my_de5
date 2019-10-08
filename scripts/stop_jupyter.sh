#!/bin/bash
export CONDA_DIR="/home/ubuntu/miniconda3"
export NOTEBOOK_DIR="/home/ubuntu"
export JUPYTER_PORT="8888"
source $CONDA_DIR/etc/profile.d/conda.sh
conda activate jupyter_env
$CONDA_DIR/envs/jupyter_env/bin/jupyter-notebook stop $JUPYTER_PORT
