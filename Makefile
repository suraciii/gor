.PHONY: test sim gen lint net fmt tidy

test:
	go test ./...

# 模拟测试跑得慢，单独一条 target，不进默认 test
sim:
	go test -tags sim -run TestSim ./sim/...

# 生成器的端到端测试要起 go list 子进程，同样不进默认 test
gen:
	go test -tags gen ./cmd/gorgen/...

# 真 TCP 的传输测试单独运行，不进默认 test
net:
	go test -tags net ./transport/...

lint:
	go vet ./...
	staticcheck ./...
	go vet -tags sim ./sim/...
	staticcheck -tags sim ./sim/...

fmt:
	gofmt -l -w .

tidy:
	go mod tidy
