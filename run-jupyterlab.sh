#!/bin/bash
nohup jupyter-lab --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token='' --no-browser & > /dev/null 2>&1