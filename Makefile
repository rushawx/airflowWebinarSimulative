SHELL := /bin/bash
PY = .venv/bin/python

.PHONY: up-services down-services up-db down-db up-af down-af lint

lint:
	pre-commit run --all-files
	pre-commit run --all-files

up-services:
	docker-compose -f docker-compose-services.yaml up -d --build

down-services:
	docker-compose -f docker-compose-services.yaml down -v

up-db:
	docker-compose -f docker-compose-db.yaml up -d --build

down-db:
	docker-compose -f docker-compose-db.yaml down -v

up-af:
	docker-compose -f docker-compose-af.yaml up -d --build

down-af:
	docker-compose -f docker-compose-af.yaml down -v
