.PHONY: build test fmt-check clippy docker helm-lint helm-smoke ci

build:
	cargo build --release

test:
	cargo test

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets -- -D warnings

ci: fmt-check clippy test helm-lint

docker:
	docker build -t duihua-background-worker:local .

helm-lint:
	helm lint charts/duihua-background-worker

helm-smoke:
	./scripts/helm-smoke-kind.sh