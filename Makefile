.PHONY: test sim gen lint net fmt fmt-check ci tidy bench

test:
	go test ./...

# 性能基线单独运行，不进默认 test；GOR_BENCH_DIR 可指定真盘目录
bench:
	go test . -run '^$$' -bench . -benchmem -count=1

# 模拟测试跑得慢，单独一条 target，不进默认 test
sim:
	go test -tags sim -run TestSim ./sim/...

# 生成器的端到端测试要起 go list 子进程，同样不进默认 test
gen:
	go test -tags gen ./cmd/gorgen/...

# 真 TCP 的传输测试单独运行，不进默认 test
net:
	go test -tags net ./transport/...
	go test -tags net ./examples/shadow/...

lint:
	go run ./internal/constraintcheck
	go vet ./...
	staticcheck ./...
	go vet -tags sim ./sim/...
	staticcheck -tags sim ./sim/...

fmt:
	gofmt -l -w .

fmt-check:
	@files="$$(gofmt -l .)"; \
	if test -n "$$files"; then \
		printf '%s\n' "$$files"; \
		exit 1; \
	fi

ci:
	$(MAKE) fmt-check
	$(MAKE) lint
	$(MAKE) test
	go test -count=1 -race ./...
	$(MAKE) sim
	$(MAKE) gen
	$(MAKE) net

tidy:
	go mod tidy
