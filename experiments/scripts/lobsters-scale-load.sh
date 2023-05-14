#!/bin/bash
K9DB_DIR="$(pwd)"
LOG_OUT="${K9DB_DIR}/experiments/scripts/logs/lobsters-scale/"
PLOT_OUT="${K9DB_DIR}/experiments/scripts/outputs/lobsters-scale/"

mkdir -p "$LOG_OUT"
mkdir -p "$PLOT_OUT"

echo "Writing plots and final outputs to $PLOT_OUT"
echo "Writing logs and temporary measurements to $LOG_OUT"
cd "${K9DB_DIR}/experiments/lobsters"

# Experiment parameters: you need to change the request scale `RS` parameter if
# running on a different configuration.
RT=120
RS=1000  # request scale
INFLIGHT=""
#INFLIGHT="--in-flight=5"
TARGET_IP="$1"

# Building harness.
bazel build -c opt //:lobsters-harness

# Loop over datascales
for DS in "1.72" "3.44" "4.31" "5.17" "6.89" "7.76" "8.62" "10.34" "12.9" "17.24"
do
  echo "Running Datascale $DS..."
  bazel run -c opt //:lobsters-harness -- \
    --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
    --backend pelton --prime --scale_everything $INFLIGHT \
    "mysql://root:password@$TARGET_IP:10001/lobsters" \
    > "$LOG_OUT/_prime_scale$DS.out" 2>&1

  bazel run -c opt //:lobsters-harness -- \
    --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
    --backend pelton --scale_everything \
    "mysql://root:password@$TARGET_IP:10001/lobsters" \
    > "$LOG_OUT/scale$DS.out" 2>&1

  # Stop pelton server.
  echo "Killing pelton server..."
  mariadb -P10001 --host=$TARGET_IP -e "STOP";

  # Sleep a while.
  sleep 20
done

#
# Plotting and other formatting.
#
echo "Experiment ran. Now plotting"
cd "${K9DB_DIR}/experiments/scripts/plotting"
. venv/bin/activate
python lobsters-scale.py --paper

echo "Success"
