#!/bin/bash

set -euo pipefail

# General arguments
ROOT=$PWD

GREEN_TEXT="\033[32m"
RESET_TEXT="\033[0m"

echo_green() {
    echo -e "$GREEN_TEXT$1$RESET_TEXT"
}

# 添加检查和清理进程的函数
check_and_cleanup_processes() {
    echo_green ">> 检查并清理已存在的进程..."
    
    # 检查是否有Python进程在使用swarm.pem
    if [ -f "$ROOT/swarm.pem" ]; then
        # Windows系统
        if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
            # 查找使用swarm.pem的Python进程
            for pid in $(tasklist /FI "IMAGENAME eq python.exe" /FO CSV | findstr /i "python.exe"); do
                pid=$(echo $pid | tr -d '"' | cut -d',' -f1)
                echo_green ">> 终止Python进程: $pid"
                taskkill /F /PID $pid 2>/dev/null || true
            done
        else
            # Linux/Mac系统
            for pid in $(lsof -t "$ROOT/swarm.pem" 2>/dev/null); do
                echo_green ">> 终止进程: $pid"
                kill -9 $pid 2>/dev/null || true
            done
        fi
        
        # 等待进程完全终止
        sleep 2
        
    fi
}

# 添加重启相关变量
MAX_RETRIES=10
RETRY_COUNT=0
RETRY_DELAY=120  # 重启等待时间（秒）


# Mac特定的内存优化设置
if [[ "$OSTYPE" == "darwin"* ]]; then
    # Mac环境变量设置
    export PYTORCH_ENABLE_MPS_FALLBACK=1
    export PYTORCH_MPS_HIGH_WATERMARK_RATIO=0.0
    export OMP_NUM_THREADS=2
    export MKL_NUM_THREADS=2
    export VECLIB_MAXIMUM_THREADS=2
    export NUMEXPR_NUM_THREADS=2
    export NUMEXPR_MAX_THREADS=2
    
    # Mac上使用不同的内存限制方式
    export PYTORCH_MPS_ALLOCATOR_POLICY=delayed
    export PYTORCH_MPS_ALLOCATOR_POLICY_MAX_ALLOCATION=6144  # 限制最大内存分配为6GB
else
    # 非Mac环境设置
    export CUDA_VISIBLE_DEVICES=0
    export PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:512
    export OMP_NUM_THREADS=4
    export MKL_NUM_THREADS=4
    ulimit -v 16000000
fi

export PUB_MULTI_ADDRS
export PEER_MULTI_ADDRS
export HOST_MULTI_ADDRS
export IDENTITY_PATH
export CONNECT_TO_TESTNET
export ORG_ID
export HF_HUB_DOWNLOAD_TIMEOUT=120

# Check if public multi-address is given else set to default
DEFAULT_PUB_MULTI_ADDRS=""
PUB_MULTI_ADDRS=${PUB_MULTI_ADDRS:-$DEFAULT_PUB_MULTI_ADDRS}

# Check if peer multi-address is given else set to default
DEFAULT_PEER_MULTI_ADDRS="/ip4/38.101.215.13/tcp/30002/p2p/QmQ2gEXoPJg6iMBSUFWGzAabS2VhnzuS782Y637hGjfsRJ" # gensyn coordinator node
PEER_MULTI_ADDRS=${PEER_MULTI_ADDRS:-$DEFAULT_PEER_MULTI_ADDRS}

# Check if host multi-address is given else set to default
DEFAULT_HOST_MULTI_ADDRS="/ip4/0.0.0.0/tcp/38331"
HOST_MULTI_ADDRS=${HOST_MULTI_ADDRS:-$DEFAULT_HOST_MULTI_ADDRS}

# Path to an RSA private key. If this path does not exist, a new key pair will be created.
# Remove this file if you want a new PeerID.
DEFAULT_IDENTITY_PATH="$ROOT"/swarm.pem
IDENTITY_PATH=${IDENTITY_PATH:-$DEFAULT_IDENTITY_PATH}

# Will ignore any visible GPUs if set.
CPU_ONLY=${CPU_ONLY:-""}

# Set if successfully parsed from modal-login/temp-data/userData.json.
ORG_ID=${ORG_ID:-""}


ROOT_DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"

# Function to clean up the server process upon exit
cleanup() {
    echo_green ">> Shutting down trainer..."

    # Remove modal credentials if they exist
    rm -r $ROOT_DIR/modal-login/temp-data/*.json 2> /dev/null || true

    # Kill all processes belonging to this script's process group
    kill -- -$$ || true

    exit 0
}

trap cleanup EXIT

# 自动设置连接选项
CONNECT_TO_TESTNET=True
echo_green ">> Automatically connecting to Testnet"

# Run modal_login server.
echo "Please login to create an Ethereum Server Wallet"
cd modal-login
# Check if the yarn command exists; if not, install Yarn.
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS specific
    [ -f ~/.zshrc ] && source ~/.zshrc
    [ -f ~/.bash_profile ] && source ~/.bash_profile
else
    # Linux/other systems
    [ -f ~/.bashrc ] && source ~/.bashrc
fi

if ! command -v yarn > /dev/null 2>&1; then
    # Detect Ubuntu (including WSL Ubuntu) and install Yarn accordingly
    if grep -qi "ubuntu" /etc/os-release 2> /dev/null || uname -r | grep -qi "microsoft"; then
        echo "Detected Ubuntu or WSL Ubuntu. Installing Yarn via apt..."
        curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
        echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
        sudo apt update && sudo apt install -y yarn
    else
        echo "Yarn is not installed. Installing Yarn..."
        curl -o- -L https://yarnpkg.com/install.sh | sh
        echo 'export PATH="$HOME/.yarn/bin:$HOME/.config/yarn/global/node_modules/.bin:$PATH"' >> ~/.bashrc
        source ~/.bashrc
    fi
fi
yarn install
yarn dev > /dev/null 2>&1 & # Run in background and suppress output

SERVER_PID=$!  # Store the process ID
echo "Started server process: $SERVER_PID"
sleep 5
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    open http://localhost:3000
elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    # Windows
    start http://localhost:3000
else
    # Linux
    xdg-open http://localhost:3000 2>/dev/null || sensible-browser http://localhost:3000 2>/dev/null || python -m webbrowser http://localhost:3000
fi
cd ..

echo_green ">> Waiting for modal userData.json to be created..."
while [ ! -f "modal-login/temp-data/userData.json" ]; do
    sleep 5  # Wait for 5 seconds before checking again
done
echo "Found userData.json. Proceeding..."

ORG_ID=$(awk 'BEGIN { FS = "\"" } !/^[ \t]*[{}]/ { print $(NF - 1); exit }' modal-login/temp-data/userData.json)
echo "Your ORG_ID is set to: $ORG_ID"

# Wait until the API key is activated by the client
echo "Waiting for API key to become activated..."
while true; do
    STATUS=$(curl -s "http://localhost:3000/api/get-api-key-status?orgId=$ORG_ID")
    if [[ "$STATUS" == "activated" ]]; then
        echo "API key is activated! Proceeding..."
        break
    else
        echo "Waiting for API key to be activated..."
        sleep 5
    fi
done

pip_install() {
    pip install --disable-pip-version-check -q -r "$1"
}

echo_green ">> Getting requirements..."
pip_install "$ROOT"/requirements-hivemind.txt
pip_install "$ROOT"/requirements.txt

if ! command -v nvidia-smi &> /dev/null; then
    # You don't have a NVIDIA GPU
    CONFIG_PATH="$ROOT/hivemind_exp/configs/mac/grpo-qwen-2.5-0.5b-deepseek-r1.yaml"
elif [ -n "$CPU_ONLY" ]; then
    # ... or we don't want to use it
    CONFIG_PATH="$ROOT/hivemind_exp/configs/mac/grpo-qwen-2.5-0.5b-deepseek-r1.yaml"
else
    # NVIDIA GPU found
    pip_install "$ROOT"/requirements_gpu.txt
    CONFIG_PATH="$ROOT/hivemind_exp/configs/gpu/grpo-qwen-2.5-0.5b-deepseek-r1.yaml"
fi

echo_green ">> Done!"

# 自动设置HF token选项
HUGGINGFACE_ACCESS_TOKEN="None"
echo_green ">> Automatically setting Hugging Face token to None"

# 添加运行函数
run_training() {
    if [ -n "$ORG_ID" ]; then
        python -m hivemind_exp.gsm8k.train_single_gpu \
            --hf_token "$HUGGINGFACE_ACCESS_TOKEN" \
            --identity_path "$IDENTITY_PATH" \
            --modal_org_id "$ORG_ID" \
            --config "$CONFIG_PATH"
    else
        python -m hivemind_exp.gsm8k.train_single_gpu \
            --hf_token "$HUGGINGFACE_ACCESS_TOKEN" \
            --identity_path "$IDENTITY_PATH" \
            --public_maddr "$PUB_MULTI_ADDRS" \
            --initial_peers "$PEER_MULTI_ADDRS" \
            --host_maddr "$HOST_MULTI_ADDRS" \
            --config "$CONFIG_PATH"
    fi
}

# 主循环
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    # 在开始训练前调用清理函数
    check_and_cleanup_processes
    echo_green ">> Starting training attempt $((RETRY_COUNT + 1)) of $MAX_RETRIES"
    
    # 运行训练
    if run_training; then
        echo_green ">> Training completed successfully"
        exit 0
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo_green ">> Training failed. Waiting $RETRY_DELAY seconds before retry..."
            sleep $RETRY_DELAY
        else
            echo_green ">> Maximum retry attempts reached. Exiting..."
            exit 1
        fi
    fi
done

wait  # Keep script running until Ctrl+C
