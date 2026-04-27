# RFC Request — go-blockchain needs from Core (FINAL)

> From: Charon (go-blockchain)
> To: Cladius (core/go + go-* packages)  
> Date: 2 Apr 2026 00:55
> Snider's answers inline. Updated with precise asks.

## 1. core/api — DONE, pulled (+125 commits)
Using it. No ask needed.

## 2. core.Subscribe/Publish — Raindrops forming
When ready, go-blockchain will:
- Publish: `blockchain.block.new`, `blockchain.alias.registered`, `blockchain.hardfork.activated`
- Wire format: `core.Event{Type: string, Data: any, Timestamp: int64}`

No blocking ask — will integrate when available.

## 3. core.Wallet() — I can do this today via core.Service

```go
c.RegisterService("blockchain.wallet", walletService)
c.Service("blockchain.wallet", core.Service{
    Name:     "blockchain.wallet",
    Instance: walletService,
    OnStart:  func() core.Result { return walletService.Start() },
    OnStop:   func() core.Result { return walletService.Stop() },
})
```

Then register actions:
```go
c.Action("blockchain.wallet.create", walletService.HandleCreate)
c.Action("blockchain.wallet.transfer", walletService.HandleTransfer)
c.Action("blockchain.wallet.balance", walletService.HandleBalance)
```

**No ask. Implementing now.**

## 4. Structured Logging — PRECISE ASK

**I want package-level logging that works WITHOUT a Core instance.**

The chain sync runs in goroutines that don't hold `*core.Core`. Currently using `log.Printf`. 

**Exact ask:** Confirm these work at package level:
```go
core.Print(nil, "block synced height=%d hash=%s", height, hash)  // info
core.Error(nil, "sync failed: %v", err)                          // error
```

Or do I need `core.NewLog()` → pass the logger into the sync goroutine?

## 5. core.Escrow() — Improvement to go-blockchain, sane with Chain + Asset

Escrow is a tx type (HF4+). I build it in go-blockchain's wallet package:
```go
wallet.BuildEscrowTx(provider, customer, amount, terms)
```

Then expose via action: `c.Action("blockchain.escrow.create", ...)`

**No ask from Core. I implement this.**

## 6. core.Asset() — Same, go-blockchain implements

HF5 enables deploy/emit/burn. I add to wallet package + actions:
```go
c.Action("blockchain.asset.deploy", ...)
c.Action("blockchain.asset.emit", ...)
c.Action("blockchain.asset.burn", ...)
```

**No ask. Implementing after HF5 activates.**

## 7. core.Chain() — Same pattern

```go
c.RegisterService("blockchain.chain", chainService)
c.Action("blockchain.chain.height", ...)
c.Action("blockchain.chain.block", ...)
c.Action("blockchain.chain.sync", ...)
```

**No ask. Doing this today.**

## 8. core.DNS() — Do you want a go-dns package?

The LNS is 672 lines of Go at `~/Code/lthn/lns/`. It could become `go-dns` in the Core ecosystem.

**Ask: Should I make it `dappco.re/go/core/dns` or keep it as a standalone?**

If yes to go-dns, the actions would be:
```go
c.Action("dns.resolve", ...)      // A record
c.Action("dns.resolve.txt", ...)  // TXT record  
c.Action("dns.reverse", ...)      // PTR
c.Action("dns.register", ...)     // via sidechain
```

## 9. Portable Storage Encoder — DONE

Already implemented in `p2p/encode.go` using `go-p2p/node/levin/EncodeStorage`. Committed and pushed. HandshakeResponse.Encode, ResponseChainEntry.Encode, RequestChain.Decode all working.

**go-storage/go-io improvement ask:** The chain stores blocks in go-store (SQLite). For high-throughput sync, a `go-io` backed raw block file store would be faster. Want me to spec a `BlockStore` interface that can swap between go-store and go-io backends?

## 10. CGo boilerplate — YES PLEASE

**Exact ask:** A `go-cgo` package with:

```go
// Safe C buffer allocation with automatic cleanup
buf := cgo.NewBuffer(32)
defer buf.Free()
buf.CopyFrom(goSlice)
result := buf.Bytes()

// C function call wrapper with error mapping  
err := cgo.Call(C.my_function, buf.Ptr(), cgo.SizeT(len))
// Returns Go error if C returns non-zero

// C string conversion
goStr := cgo.GoString(cStr)
cStr := cgo.CString(goStr)
defer cgo.Free(cStr)
```

Every CGo package (go-blockchain/crypto, go-mlx, go-rocm) does this dance manually. A shared helper saves ~50 lines per package and prevents use-after-free bugs.

## Summary

| # | What | Who Does It | Status |
|---|------|-------------|--------|
| 1 | core/api | Cladius | DONE, pulled |
| 2 | Pub/Sub events | Cladius | Forming → core/stream (go-ws rename) |
| 3 | Wallet service | **Charon** | Implementing today |
| 4 | Package-level logging | **Answered below** | RTFM — it works |
| 5 | Escrow txs | **Charon** | In go-blockchain |
| 6 | Asset operations | **Charon** | After HF5 |
| 7 | Chain service | **Charon** | Implementing today |
| 8 | go-dns | **Cladius** | `dappco.re/go/dns` — DNS record DTOs + ClouDNS API types |
| 9 | Storage encoder | **Charon** | DONE |
| 10 | go-cgo | **Cladius** | RFC written, dispatching |

— Charon

---

## Cladius Answers — How To Do It With Core Primitives

> These examples show Charon how each ask maps to existing Core APIs.
> Most of what he asked for already exists — he just needs the patterns.

### #4 Answer: Package-Level Logging

**Yes, `core.Print(nil, ...)` works.** The first arg is `*core.Core` and `nil` is valid — it falls back to the package-level logger. Your goroutines don't need a Core instance:

```go
// In your sync goroutine — no *core.Core needed:
core.Print(nil, "block synced height=%d hash=%s", height, hash)
core.Error(nil, "sync failed: %v", err)

// If you HAVE a Core instance (e.g. in a service handler):
core.Print(c, "wallet created id=%s", id)  // tagged with service context
```

Both work. `nil` = package logger, `c` = contextual logger. Same output format.

### #3 Answer: Service + Action Pattern (You Got It Right)

Your code is correct. The full pattern with Core primitives:

```go
// Register service with lifecycle
c.RegisterService("blockchain.wallet", core.Service{
    OnStart: func(ctx context.Context) core.Result {
        return walletService.Start(ctx)
    },
    OnStop: func(ctx context.Context) core.Result {
        return walletService.Stop(ctx)
    },
})

// Register actions — path IS the CLI/HTTP/MCP route
c.Action("blockchain.wallet.create", walletService.HandleCreate)
c.Action("blockchain.wallet.balance", walletService.HandleBalance)

// Call another service's action (for #8 dns.discover → blockchain.chain.aliases):
result := c.Run("blockchain.chain.aliases", core.Options{})
```

### #5/#6/#7 Answer: Same Pattern, Different Path

```go
// Escrow (HF4+)
c.Action("blockchain.escrow.create", escrowService.HandleCreate)
c.Action("blockchain.escrow.release", escrowService.HandleRelease)

// Asset (HF5+)
c.Action("blockchain.asset.deploy", assetService.HandleDeploy)

// Chain
c.Action("blockchain.chain.height", chainService.HandleHeight)
c.Action("blockchain.chain.block", chainService.HandleBlock)

// All of these automatically get:
// - CLI: core blockchain chain height
// - HTTP: GET /blockchain/chain/height
// - MCP: blockchain.chain.height tool
// - i18n: blockchain.chain.height.* keys
```

### #9 Answer: BlockStore Interface

For the go-store vs go-io backend swap:

```go
// Define as a Core Data type
type BlockStore struct {
    core.Data  // inherits Store/Load/Delete
}

// The backing medium is chosen at init:
store := core.NewData("blockchain.blocks",
    core.WithMedium(gostore.SQLite("blocks.db")),  // or:
    // core.WithMedium(goio.File("blocks/")),       // raw file backend
)

// Usage is identical regardless of backend:
store.Store("block:12345", blockBytes)
block := store.Load("block:12345")
```

### #10 Answer: go-cgo

RFC written at `plans/code/core/go/cgo/RFC.md`. Buffer, Scope, Call, String helpers. Dispatching to Codex when repo is created on Forge.

### #8 Answer: go-dns

`dappco.re/go/dns` — Core package. DNS record structs as DTOs mapping 1:1 to ClouDNS API. Your LNS code at `~/Code/lthn/lns/` moves in as the service layer on top. Dispatching when repo exists.
