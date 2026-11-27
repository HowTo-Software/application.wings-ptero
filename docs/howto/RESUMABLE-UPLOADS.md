# Uploads fragmentados e retomáveis (build HowTo.Software do Wings) / Chunked, resumable uploads

Base: **Wings v1.13.4** (tag `v1.13.4`, branch `howto/v1.13.4`). Mudança **aditiva**: `POST /upload/file`, SFTP, a API do painel e
todos os outros endpoints não mudam. Instalação/reversão: [INSTALL-AND-ROLLBACK.md](./INSTALL-AND-ROLLBACK.md).
O lado do painel está em `HowTo-Software/application.panel-ptero` (`docs/uploads/`).

Português primeiro; English at the end.

## 1. O que foi acrescentado

| Arquivo | O quê |
|---|---|
| `server/uploadsession/` | Núcleo: sessões de partes de 16 MiB (qualquer ordem, em paralelo), SHA-256 por parte e **incremental** do arquivo inteiro (estado persistido), retomada após reinício, troca atômica, limpeza por TTL. Depende só de uma interface pequena de filesystem → testável sem Wings. |
| `router/router_upload_session.go` | Rotas `POST/GET/PUT/DELETE /upload/session…`, autorização por JWT, mapeamento de erros (`code` legível por máquina), varredura periódica, anúncio em `/api/system?v=2`. |
| `router/tokens/upload_session.go` | Payload do JWT `upload-session` (multiuso, preso a sessão/servidor/usuário/pasta/nome/tamanho). |
| `server/filesystem/chunked.go`, `internal/ufs/rename_replace_unix.go`, `fs_quota.go` | `OpenForChunkedWrite` (escrita contabilizada na cota, sem truncar), `ReplaceFile` (troca atômica via `renameat`, só arquivo regular, preserva permissões, ajusta a cota). |
| `config/config.go`, `cmd/root.go`, `router/router.go` | Bloco `api.upload_sessions.*`; HTTP/1.1-only opcional; janela de recepção HTTP/2 opcional. |
| `tools/uploadbench/`, `.github/workflows/howto-*.yaml` | Ferramenta e workflows de CI/benchmark. |

Protocolo, garantias e limites: ver `docs/uploads/WINGS-RESUMABLE-UPLOADS.md` no painel.

## 2. Testes

* `server/uploadsession`: 18 funções de teste (24 casos contando subtestes) (ordem aleatória + 8 gravadores paralelos, retomada após reinício com hash restaurado, partes corrompidas/curtas/longas, idempotência, completar incompleto, hash do arquivo inteiro, substituição com/sem permissão, isolamento entre usuários e servidores, nomes/diretórios maliciosos, cota, `.part` apagado pelo dono do servidor, varredura/TTL/lápides, partes concorrentes de várias sessões, mesma parte duas vezes ao mesmo tempo, reenvio do mesmo arquivo). Rodam no Windows/Linux e com `-race`.
* `router`: 11 testes de integração HTTP sobre o roteador real e um volume real (fluxo paralelo completo, retomada após reinício, regras de token, partes inválidas, limites, `replace` preservando modo, contenção na raiz do servidor, abortar, **upload legado intacto**, feature desligável, capacidades anunciadas).
* CI (`HowTo CI`, ubuntu-24.04, amd64+arm64): `go vet`, **toda a suíte upstream** (incluindo `sftp`, `filesystem`, `ufs`, `router`) e `go test -race`.

## 3. Benchmark (rede **emulada** em runner compartilhado do GitHub — não é a internet)

**Como ler:** veth + namespace de rede + `tc netem` (atraso **nos dois sentidos**: RTT = 2 × atraso; perda **em cada sentido**), MTU 1500, TSO/GSO/GRO desligados, `netdev_max_backlog` elevado para o `netem` não descartar rajadas. O servidor é o **roteador real** deste repositório num processo, o cliente outro processo; cada execução envia arquivo de 192 MiB (12 partes) com limite de 60 s e **2 rodadas**. A vazão é medida **no cliente** (bytes entregues ao TCP ÷ tempo); em execuções cortadas pelo limite de tempo isso inclui o que ainda estava em buffers do kernel (poucos MiB), então a vazão de execuções lentas é levemente otimista. CPU/RAM/disco são do processo do servidor (`/proc/<pid>`); retransmissões são do TCP do cliente (quem envia os dados). Runners são máquinas virtuais compartilhadas: **CPU e disco são ruidosos** e o hardware varia entre perfis (veja o CPU de cada tabela). **Os números descrevem esta emulação; não preveem o caminho real entre um usuário e o node.**

<!-- BENCH -->

## 4. Limitações do que foi medido

* Emulação de atraso/perda **uniforme e aleatória**; a internet tem perda em rajadas, variação de atraso e filas.
* O cliente do benchmark é Go; um **navegador** tem seu próprio TCP, limite de 6 conexões HTTP/1.1 por host e negocia HTTP/2 se o servidor oferecer.
* Execuções limitadas a 60 s por configuração; arquivos de 192 MiB; 2 rodadas (variação indicada por min–máx).
* A vazão do **algoritmo adaptativo do painel** não foi medida aqui (o cliente do benchmark usa um número fixo de conexões); ele foi validado por simulação nos testes do painel.
* Nenhuma medição em produção; nenhuma com o caminho real Brasil → node1.

---

## English

Additive build on **Wings v1.13.4**: the legacy upload, SFTP and all other endpoints are untouched. See the table in §1 for what was added (`server/uploadsession`, router handlers, JWT payload, filesystem primitives `OpenForChunkedWrite` / `ReplaceFile` / `RenameReplace`, `api.upload_sessions.*` config, benchmark tool and workflows). Tests: 18 session-logic test functions (Windows/Linux, `-race`), 11 HTTP integration tests on the real router and a real volume, and the whole upstream suite on linux amd64/arm64 in CI.

**Benchmark** (§3): an *emulated* network (veth + netns + `tc netem`, delay and loss in both directions, MTU 1500, offloads off) on shared GitHub runners; the real router in one process, a Go client in another; 192 MiB files, 60 s time box, 2 runs. Throughput is measured at the client; CPU/RAM/disk are the server process's; retransmissions are the client TCP's. **The numbers describe this emulation, not the real internet path**, and CPU/disk figures are noisy on shared hardware. Limitations (§4): uniform random loss, Go client rather than a browser, short runs, the panel's adaptive algorithm not measured here (validated by simulation), nothing measured in production or on the real Brazil → node1 path.
