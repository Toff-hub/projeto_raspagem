SHELL := /bin/bash

ifeq ($(wildcard venv/bin/python),venv/bin/python)
	VENV := venv
else
	VENV := .venv
endif

PIP := $(VENV)/bin/pip
UVICORN := $(VENV)/bin/uvicorn

APP := app.main:app
HOST := 127.0.0.1
PORT := 9090

.PHONY: help install start dev doctor freeze clean clean-temp recreate-venv

help:
	@echo "Comandos disponíveis:"
	@echo "  make install        - instala dependências e configura o projeto"
	@echo "  make start          - inicia o FastAPI em modo dev"
	@echo "  make dev            - alias para start"
	@echo "  make doctor         - roda diagnóstico do crawl4ai"
	@echo "  make freeze         - atualiza o requirements.txt com o ambiente atual"
	@echo "  make clean-temp     - limpa PDFs temporários e outputs"
	@echo "  make clean          - limpa caches Python"
	@echo "  make recreate-venv  - apaga e recria o ambiente virtual"

install:
	chmod +x install.sh
	./install.sh

start:
	$(UVICORN) $(APP) --host $(HOST) --port $(PORT) --reload

dev: start

doctor:
	@if [ -x "$(VENV)/bin/crawl4ai-doctor" ]; then \
		$(VENV)/bin/crawl4ai-doctor; \
	else \
		echo "crawl4ai-doctor não encontrado. Rode: make install"; \
	fi

freeze:
	$(PIP) freeze > requirements.txt

clean-temp:
	rm -rf app/temp/*
	rm -rf app/output/*

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.log" -delete

recreate-venv:
	rm -rf venv .venv
	$(MAKE) install