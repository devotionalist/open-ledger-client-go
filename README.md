# open-ledger-client-go

open-ledger 的 Go 客户端，和 [open-ledger-client-rust](../open-ledger-client-rust) / [open-ledger-client-java](../open-ledger-client-java) / [open-ledger-client-ts](../open-ledger-client-ts) 同一套 quorum 规则。

Bookie 只存 entry。本库负责选 write quorum、等 Qa、fence coverage、向前读修并 CAS `CLOSED`。

`BookieAsync` 是 Callback API（投递 + 完成回调）；`Bookie` 是阻塞适配。`MemoryBookie` 供测试；`GRPCBookie`（`DialBookie`）连 `openledgerd`，Async* 为主、同步方法为薄适配。

## Layout

```text
.
├── bookie.go / client.go / meta.go / …   # 根包 ledgerclient
├── grpc_bookie.go                       # gRPC Bookie
├── proto/bookie/v1/bookie.proto         # 与 open-ledger 同步
├── internal/bookiepb/                   # protoc 生成代码
└── integration/                         # 本地 k8s（-tags=integration）
```

## In-process

```go
nodes := []*ledgerclient.MemoryBookie{
    ledgerclient.NewMemoryBookie(),
    ledgerclient.NewMemoryBookie(),
    ledgerclient.NewMemoryBookie(),
}
ids := []string{"b0", "b1", "b2"}
bookies := map[string]ledgerclient.Bookie{}
for i, id := range ids {
    bookies[id] = nodes[i]
}
metas := ledgerclient.NewMemoryMetaStore()
writer, err := ledgerclient.Create(metas, bookies, 1, ledgerclient.Quorum{E: 3, Qw: 3, Qa: 2}, ids, []byte("secret"))
id, err := writer.Append([]byte("hello"))
last, err := writer.Close()
```

## gRPC (Callback API)

```go
b0, err := ledgerclient.DialBookie(ctx, "127.0.0.1:3181")
defer b0.Close()

// Async (preferred on hot path)
b0.AddEntryAsync(ctx, ledgerID, entryID, lac, payload, key, func(err error) { /* Finish */ })

// Sync adapter (process edges / existing quorum helpers)
bookies := map[string]ledgerclient.Bookie{"bookie-0": b0}
```

## Tests

```bash
go test ./...

# 本地 Colima k3s，namespace open-ledger，bookie-0/1 Ready 且监听 3181
go test -tags=integration ./integration/ -count=1 -v
```

集成测试覆盖：单 writer 连续写、两 writer 并发写两个 ledger、recover 后旧 writer 被 fence、脏尾读修、并发 recover 收敛。对两个 Pod 做一次 `kubectl port-forward` 后复用。也可预设：

```bash
export OPEN_LEDGER_BOOKIE_ADDRS=127.0.0.1:13181,127.0.0.1:13182
```

## Continuous traffic (fault companion)

```bash
# 先 port-forward 三个 bookie，再持续 append/read（E=Qw=3,Qa=2）
go run ./cmd/traffic \
  --addrs=127.0.0.1:13181,127.0.0.1:13182,127.0.0.1:13183 \
  --ids=bookie-0,bookie-1,bookie-2 \
  --duration=60s
```

与 open-ledger 仓库联动：

```bash
../open-ledger/scripts/k8s-traffic-fault.sh   # C06 持续写
../open-ledger/scripts/k8s-recover-fault.sh   # R01–R06 recover 矩阵
```

集成默认 **3 bookie**，`E=Qw=3,Qa=2`（striping 用 `E=3,Qw=2,Qa=2`）。包含：确认 entry recover、杀 Pod 后 recover、缺成员 `IN_RECOVERY`、脏尾 repair、旧 writer fence。

## License

Apache-2.0
