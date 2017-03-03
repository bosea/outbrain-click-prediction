#!/usr/bin/env bash
#
# Script for launching Spark jobs for Kaggle Outbrain click prediction modeling
#
# History:
# 09/25/2014 abose "created"

echo 'Running Kaggle Outbrain click prediction - Model Input Prep Pipeline'

VAR1=`hostname`

export SAMPLE_RATIO=0.001
export SPLIT_RATIO=0.7

# HDFS for datasets and checkpointing directories
export CHECKPOINT_DIR=/home/$USER/outbrain/checkpoint
export INPUT_DIR=/home/$USER/outbrain/original
export SAMPLED_INPUT_DIR=/home/$USER/outbrain/sampled_${SAMPLE_RATIO}
export MODEL_INPUT_DIR=/home/$USER/outbrain/model_input

echo ${SAMPLE_RATIO}
echo ${CHECKPOINT_DIR}
echo ${SAMPLED_INPUT_DIR}
echo ${INPUT_DIR}
echo ${MODEL_INPUT_DIR}

hadoop dfs -rm -r -f ${CHECKPOINT_DIR}
hadoop dfs -mkdir ${CHECKPOINT_DIR}


${SPARK_HOME}/bin/spark-submit --class OutbrainRFModel --deploy-mode client --driver-memory 1g --executor-memory 1g  --master spark://${VAR1}:7077 /home/$USER/IdeaProjects/outbrain-random-forest/target/Modeling-1.1.0.jar ${MODEL_INPUT_DIR} ${MODEL_INPUT_DIR}/train_0.001.csv  ${MODEL_INPUT_DIR}/test_0.001.csv ${CHECKPOINT_DIR} 100 3 5 10 false 50,100,400 3,7,10 2,5
