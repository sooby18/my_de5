#!/bin/bash
export CONDA_DIR="/home/ubuntu/miniconda3"
export NOTEBOOK_DIR="/home/ubuntu"
source $CONDA_DIR/etc/profile.d/conda.sh
conda activate jupyter_env
nohup $CONDA_DIR/envs/jupyter_env/bin/jupyter-notebook --no-browser --notebook-dir=$NOTEBOOK_DIR 1>jupyter_notebook.out 2>jupyter_notebook.err &
