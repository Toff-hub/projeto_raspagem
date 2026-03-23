#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ -d "${PROJECT_ROOT}/venv" ]]; then
  VENV_DIR="${PROJECT_ROOT}/venv"
else
  VENV_DIR="${PROJECT_ROOT}/.venv"
fi

echo "=================================================="
echo " PDF Master Builder - Instalação"
echo " Projeto: ${PROJECT_ROOT}"
echo " Venv: ${VENV_DIR}"
echo "=================================================="

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

require_command() {
  if ! command_exists "$1"; then
    echo "Erro: comando obrigatório não encontrado: $1"
    exit 1
  fi
}

install_apt_packages() {
  echo ""
  echo "[1/6] Instalando dependências de sistema via apt..."

  sudo apt update

  sudo apt install -y \
    python3 \
    python3-venv \
    python3-pip \
    curl \
    wget \
    ca-certificates \
    gnupg \
    build-essential \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libharfbuzz0b \
    libpangoft2-1.0-0 \
    libffi-dev \
    libjpeg-dev \
    libopenjp2-7-dev \
    libxml2 \
    libxslt1.1 \
    shared-mime-info
}

create_venv() {
  echo ""
  echo "[2/6] Preparando ambiente virtual Python..."
  if [[ ! -d "${VENV_DIR}" ]]; then
    "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  fi

  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"

  python -m pip install --upgrade pip setuptools wheel
}

install_python_requirements() {
  echo ""
  echo "[3/6] Instalando dependências Python..."

  if [[ ! -f "${PROJECT_ROOT}/requirements.txt" ]]; then
    echo "Erro: requirements.txt não encontrado em ${PROJECT_ROOT}"
    exit 1
  fi

  pip install -r "${PROJECT_ROOT}/requirements.txt"
}

ensure_project_dirs() {
  echo ""
  echo "[4/6] Garantindo estrutura de pastas..."
  mkdir -p "${PROJECT_ROOT}/app/static"
  mkdir -p "${PROJECT_ROOT}/app/output"
  mkdir -p "${PROJECT_ROOT}/app/temp"
}

setup_crawl4ai() {
  echo ""
  echo "[5/6] Configurando crawl4ai/browser..."

  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"

  if command_exists crawl4ai-setup; then
    crawl4ai-setup || true
  else
    echo "Aviso: crawl4ai-setup não encontrado. Tentando fallback via Playwright..."
  fi

  python -m playwright install chromium || true

  if command_exists crawl4ai-doctor; then
    echo ""
    echo "Executando diagnóstico do crawl4ai..."
    crawl4ai-doctor || true
  fi
}

final_instructions() {
  echo ""
  echo "[6/6] Instalação finalizada."
  echo ""
  echo "Ative o ambiente virtual:"
  echo "  source ${VENV_DIR}/bin/activate"
  echo ""
  echo "Para iniciar:"
  echo "  make start"
  echo ""
  echo "Ou diretamente:"
  echo "  ${VENV_DIR}/bin/uvicorn app.main:app --reload"
  echo ""
  echo "Abra no navegador:"
  echo "  http://127.0.0.1:8000"
  echo ""
}

main() {
  require_command "${PYTHON_BIN}"

  if command_exists apt; then
    install_apt_packages
  else
    echo "Erro: este script foi preparado para Ubuntu/Debian/WSL com apt."
    exit 1
  fi

  create_venv
  install_python_requirements
  ensure_project_dirs
  setup_crawl4ai
  final_instructions
}

main "$@"