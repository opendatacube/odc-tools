.PHONY: test

DEV=--file docker-compose.yml --file docker-compose.dev.yml

build:
	docker-compose build

test:
	docker-compose up -d
	docker-compose exec -T tools-tester run_tests.sh
	docker-compose down

up:
	docker-compose ${DEV} \
		up

shell:
	docker-compose ${DEV} exec tools-tester bash
