.PHONY: build
build:
	@docker run \
		--rm \
		-e CGO_ENABLED=0 \
		-v $(PWD):/usr/src/concord-controller \
		-w /usr/src/concord-controller \
		golang /bin/sh -c "go get -v -d && go build -a -installsuffix cgo -o main"
	@docker build -t concord/controller .
	@rm -f main

.PHONY: mock
mock: 
	@go get github.com/vektra/mockery/.../
	@mockery -all -inpkg -testonly

.PHONY: test
test:
	@docker run \
		-d \
		-e ARANGO_ROOT_PASSWORD=abc123 \
		--name concord-controller_test__arangodb \
		arangodb/arangodb
	@docker run \
		-d \
		-e ARANGODB_HOST=http://arangodb:8529 \
		-e ARANGODB_NAME=test__concord_controller \
		-e ARANGODB_USER=root \
		-e ARANGODB_PASS=abc123 \
		-v $(PWD):/go/src/concord-controller \
		-v $(PWD)/.src:/go/src \
		-w /go/src/concord-controller \
		--link concord-controller_test__arangodb:arangodb \
		--name concord-controller_test \
		golang /bin/sh -c "go get -v -t -d && go test -v -coverprofile=.coverage.out"
	@docker logs -f concord-controller_test
	@docker rm -f concord-controller_test
	@docker rm -f concord-controller_test__arangodb

.PHONY: test-short
test-short:
	@docker run \
		--rm \
		-it \
		-e CONCORD_STATUS_CHANGE_NOTIFIER_HOST=concord-status-change-notifier \
		-e CONCORD_PRIORITY_QUEUE_HOST=concord-priority-queue \
		-e CONCORD_TIMETABLE_HOST=concord-timetable \
		-v $(PWD):/go/src/concord-controller \
		-v $(PWD)/.src:/go/src \
		-w /go/src/concord-controller \
		golang /bin/sh -c "go get -v -t -d && go test -short -v"
