.PHONY: test sim gen lint fmt tidy

test:
	go test ./...

# 模拟测试跑得慢，单独一条 target，不进默认 test
sim:
	go test -tags sim -run TestSim ./sim/...

# 生成器的端到端测试要起 go list 子进程，同样不进默认 test
gen:
	go test -tags gen ./cmd/gorgen/...

lint:
	go vet ./...
	staticcheck ./...
	go vet -tags sim ./sim/...
	staticcheck -tags sim ./sim/...

fmt:
	gofmt -l -w .

tidy:
	go mod tidy
