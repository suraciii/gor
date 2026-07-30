.PHONY: test sim lint fmt tidy

test:
	go test ./...

# 模拟测试跑得慢，单独一条 target，不进默认 test
sim:
	go test -tags sim -run TestSim ./sim/...

lint:
	go vet ./...
	staticcheck ./...

fmt:
	gofmt -l -w .

tidy:
	go mod tidy
