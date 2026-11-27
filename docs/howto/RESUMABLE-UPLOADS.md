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

**Como ler:** veth + namespace de rede + `tc netem` (atraso **nos dois sentidos**: RTT = 2 × atraso; perda **em cada sentido**), MTU 1500, TSO/GSO/GRO desligados, `netdev_max_backlog` elevado para o `netem` não descartar rajadas. O servidor é o **roteador real** deste repositório num processo, o cliente outro processo; cada execução envia arquivo de 192 MiB (12 partes) com limite de 60 s e **3 rodadas** (cache de métricas TCP do kernel limpo antes de cada rodada). A vazão é medida **no cliente** (bytes entregues ao TCP ÷ tempo); em execuções cortadas pelo limite de tempo isso inclui o que ainda estava em buffers do kernel (poucos MiB), então a vazão de execuções lentas é levemente otimista. CPU/RAM/disco são do processo do servidor (`/proc/<pid>`); retransmissões são do TCP do cliente (quem envia os dados). Runners são máquinas virtuais compartilhadas: **CPU e disco são ruidosos** e o hardware varia entre perfis (veja o CPU de cada tabela). **Os números descrevem esta emulação; não preveem o caminho real entre um usuário e o node.**

**Conclusões (run final: 3 rodadas por configuração, mediana; leia as tabelas para a variação):**

1. **HTTP/2 limita um upload a janela ÷ RTT.** A 116 ms sem perda, o upload legado em h2 ficou em **8,4 MB/s** contra 23,3 MB/s em HTTP/1.1; com `http2_receive_window_mib: 16` foi a **23,8 MB/s** (≈2,8×). O chunked com 8 requisições em h2 também ficou em 8,4 MB/s (25,7 com a janela de 16 MiB): em h2 as requisições paralelas dividem **uma** conexão TCP e **uma** janela.
2. **Conexões HTTP/1.1 separadas somam.** A 116 ms sem perda, chunked com 1/2/4/8 conexões: **19,8 / 33,4 / 47,7 / 51,4 MB/s** (a 8 há saturação perto de 51). Com perda de 0,5 %: **0,37 / 0,78 / 1,67 / 3,20 MB/s** (≈ linear). Com 0,1 % de perda e teto de 100 Mbit/s: 1,55 / 2,47 / 4,02 / 6,00 MB/s.
3. **Com 0,1 % de perda** os resultados variam muito entre rodadas (ex.: 2 conexões: 14,0–28,7 MB/s; 4 conexões: 10,7–37,0 MB/s), por isso **não há escalonamento monótono confiável** entre 2, 4 e 8 conexões; o que a medição sustenta é que **todas as configurações com 2+ conexões HTTP/1.1 (11,9–22,0 MB/s) ficaram ≥3,6× acima das de 1 conexão (1,2–3,3 MB/s)** e foram as únicas que terminaram os 192 MiB em 60 s.
4. **Uma conexão em chunks é ~15 % mais lenta que um único POST** (19,8 vs 23,3 MB/s a 116 ms): compatível com o intervalo de resposta entre partes consecutivas na mesma conexão (HTTP/1.1 não faz pipeline), **não provado**. A hipótese de *slow start após ociosidade* foi **testada e refutada** (`tcp_slow_start_after_idle=0` não mudou: 20,3 vs 19,8 MB/s).
5. **Custos no node** (processo do servidor): ~2,5–3,7 ms de CPU por MB sem emulação e 5–10 ms/MB nos perfis lentos (mais tempo ocioso/sobrecarga por MB; runners compartilhados, ruidoso); **RAM de pico 29–36 MiB** em HTTP/1.1 e até **52 MiB** com 8 streams h2 e janela de 16 MiB (o custo da janela); **escrita em disco = tamanho do arquivo** (192,1 MiB para 192 MiB: sem amplificação).
6. **Sem emulação** (veth, 4 vCPU) todas as configurações ficaram entre ~120 e ~147 MB/s (limite de CPU do runner): o paralelismo não ajuda quando não há RTT/perda, e a sessão em chunks não custa vazão relevante.

**O que isto não permite afirmar:** quanto isso melhora o caminho real Brasil → node1 (perda em rajadas, filas, RTT variável); o comportamento de um navegador (6 conexões HTTP/1.1 por host; negocia h2 se oferecido); nem o algoritmo adaptativo do painel. A recomendação operacional decorrente é **testar em homologação, no caminho real, `disable_http2: true` (ou janela h2 maior) com o cliente do painel**, e só então decidir.

### baseline — veth, sem emulação (nenhum atraso/perda)

Runner: AMD EPYC 9V45 96-Core Processor · 4 vCPU · kernel 7.0.0-1020-azure · netem `none`

| variante | modo | proto | conexões | rodadas completas | vazão MB/s (mediana) | min–máx | retransmissões % | CPU servidor ms/MB | RAM servidor pico MiB | disco escrito MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline+h2win16 | legacy | h2 | 1 | 3/3 | 141.51 | 141.35–144.87 | 0.166 | 5.02 | 34.4 | 192.0 |
| baseline+h2win16 | chunked | h2 | 1 | 3/3 | 109.83 | 109.52–124.51 | 0.013 | 3.53 | 38.3 | 192.1 |
| baseline+h2win16 | chunked | h2 | 8 | 3/3 | 137.33 | 129.52–140.60 | 0.080 | 3.28 | 52.4 | 192.1 |
| baseline+ssai0 | chunked | h1 | 1 | 3/3 | 128.44 | 115.18–129.43 | 0.000 | 2.93 | 30.5 | 192.1 |
| baseline | legacy | h1 | 1 | 3/3 | 125.43 | 122.73–153.28 | 0.000 | 2.83 | 29.2 | 192.0 |
| baseline | legacy | h2 | 1 | 3/3 | 135.25 | 128.07–145.83 | 0.122 | 5.07 | 32.9 | 192.0 |
| baseline | chunked | h1 | 1 | 3/3 | 122.58 | 99.05–123.84 | 0.000 | 3.03 | 33.7 | 192.1 |
| baseline | chunked | h1 | 2 | 3/3 | 125.86 | 123.24–136.70 | 0.003 | 3.23 | 34.0 | 192.1 |
| baseline | chunked | h1 | 4 | 3/3 | 135.16 | 132.70–151.04 | 0.001 | 3.53 | 35.2 | 192.1 |
| baseline | chunked | h1 | 8 | 3/3 | 146.71 | 145.39–149.22 | 0.041 | 3.73 | 36.1 | 192.1 |
| baseline | chunked | h2 | 8 | 3/3 | 128.24 | 127.30–133.85 | 0.244 | 3.38 | 38.3 | 192.1 |

### rtt116 — RTT 116 ms (58 ms por sentido), sem perda adicional

Runner: AMD EPYC 9V45 96-Core Processor · 4 vCPU · kernel 7.0.0-1020-azure · netem `delay 58ms`

| variante | modo | proto | conexões | rodadas completas | vazão MB/s (mediana) | min–máx | retransmissões % | CPU servidor ms/MB | RAM servidor pico MiB | disco escrito MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| rtt116+h2win16 | legacy | h2 | 1 | 3/3 | 23.76 | 23.74–24.01 | 0.000 | 4.52 | 36.1 | 192.0 |
| rtt116+h2win16 | chunked | h2 | 1 | 3/3 | 20.67 | 20.07–20.93 | 0.000 | 3.38 | 37.9 | 192.1 |
| rtt116+h2win16 | chunked | h2 | 8 | 3/3 | 25.73 | 25.55–25.86 | 0.000 | 3.23 | 40.6 | 192.1 |
| rtt116+ssai0 | chunked | h1 | 1 | 3/3 | 20.32 | 20.25–20.45 | 0.000 | 2.38 | 30.4 | 192.1 |
| rtt116 | legacy | h1 | 1 | 3/3 | 23.29 | 21.09–23.53 | 0.000 | 2.19 | 28.9 | 192.0 |
| rtt116 | legacy | h2 | 1 | 3/3 | 8.37 | 8.32–8.38 | 0.000 | 6.51 | 32.6 | 192.0 |
| rtt116 | chunked | h1 | 1 | 3/3 | 19.80 | 19.77–20.49 | 0.000 | 2.53 | 32.8 | 192.1 |
| rtt116 | chunked | h1 | 2 | 3/3 | 33.39 | 31.81–34.32 | 0.000 | 2.53 | 33.6 | 192.1 |
| rtt116 | chunked | h1 | 4 | 3/3 | 47.74 | 47.30–50.55 | 0.000 | 2.58 | 35.0 | 192.1 |
| rtt116 | chunked | h1 | 8 | 3/3 | 51.44 | 48.69–55.22 | 0.123 | 2.83 | 35.9 | 192.1 |
| rtt116 | chunked | h2 | 8 | 3/3 | 8.40 | 8.37–8.41 | 0.000 | 3.23 | 38.7 | 192.1 |

### rtt116-loss0.1 — RTT 116 ms, perda 0,1 % por sentido

Runner: Intel(R) Xeon(R) 6973P-C · 4 vCPU · kernel 7.0.0-1020-azure · netem `delay 58ms loss 0.1%`

| variante | modo | proto | conexões | rodadas completas | vazão MB/s (mediana) | min–máx | retransmissões % | CPU servidor ms/MB | RAM servidor pico MiB | disco escrito MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| rtt116-loss0.1+h2win16 | legacy | h2 | 1 | 2/3 | 5.35 | 2.56–14.20 | 0.077 | 5.36 | 35.6 | 192.0 |
| rtt116-loss0.1+h2win16 | chunked | h2 | 1 | 0/3 | 1.85 | 0.80–2.34 | 0.146 | 4.42 | 38.0 | 115.8 |
| rtt116-loss0.1+h2win16 | chunked | h2 | 8 | 2/3 | 3.74 | 1.38–3.96 | 0.097 | 4.32 | 38.0 | 192.1 |
| rtt116-loss0.1+ssai0 | chunked | h1 | 1 | 0/3 | 1.06 | 0.96–2.57 | 0.096 | 4.49 | 28.7 | 60.1 |
| rtt116-loss0.1 | legacy | h1 | 1 | 0/3 | 1.23 | 0.89–3.03 | 0.098 | 3.38 | 27.7 | 66.9 |
| rtt116-loss0.1 | legacy | h2 | 1 | 0/3 | 1.67 | 1.29–1.96 | 0.094 | 7.75 | 30.7 | 94.7 |
| rtt116-loss0.1 | chunked | h1 | 1 | 0/3 | 3.25 | 0.97–3.33 | 0.093 | 3.11 | 31.8 | 184.5 |
| rtt116-loss0.1 | chunked | h1 | 2 | 3/3 | 21.99 | 14.00–28.67 | 0.107 | 2.53 | 32.8 | 192.1 |
| rtt116-loss0.1 | chunked | h1 | 4 | 3/3 | 11.94 | 10.74–37.02 | 0.086 | 2.68 | 33.8 | 192.1 |
| rtt116-loss0.1 | chunked | h1 | 8 | 3/3 | 15.22 | 7.56–20.23 | 0.152 | 2.83 | 36.1 | 192.1 |
| rtt116-loss0.1 | chunked | h2 | 8 | 0/3 | 1.53 | 1.14–1.62 | 0.109 | 4.47 | 37.0 | 87.0 |

### rtt116-loss0.5 — RTT 116 ms, perda 0,5 % por sentido

Runner: AMD EPYC 9V74 80-Core Processor · 4 vCPU · kernel 7.0.0-1020-azure · netem `delay 58ms loss 0.5%`

| variante | modo | proto | conexões | rodadas completas | vazão MB/s (mediana) | min–máx | retransmissões % | CPU servidor ms/MB | RAM servidor pico MiB | disco escrito MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| rtt116-loss0.5+h2win16 | legacy | h2 | 1 | 0/3 | 0.37 | 0.33–0.47 | 0.507 | 15.26 | 28.8 | 20.6 |
| rtt116-loss0.5+h2win16 | chunked | h2 | 1 | 0/3 | 0.54 | 0.44–0.72 | 0.455 | 7.15 | 32.5 | 31.8 |
| rtt116-loss0.5+h2win16 | chunked | h2 | 8 | 0/3 | 0.41 | 0.40–0.61 | 0.494 | 6.24 | 34.7 | 23.0 |
| rtt116-loss0.5+ssai0 | chunked | h1 | 1 | 0/3 | 0.35 | 0.34–0.36 | 0.514 | 8.82 | 28.1 | 19.2 |
| rtt116-loss0.5 | legacy | h1 | 1 | 0/3 | 0.48 | 0.36–1.07 | 0.511 | 8.68 | 27.7 | 28.4 |
| rtt116-loss0.5 | legacy | h2 | 1 | 0/3 | 0.37 | 0.36–0.49 | 0.493 | 15.63 | 29.7 | 20.7 |
| rtt116-loss0.5 | chunked | h1 | 1 | 0/3 | 0.37 | 0.37–1.07 | 0.454 | 8.13 | 30.6 | 20.8 |
| rtt116-loss0.5 | chunked | h1 | 2 | 0/3 | 0.78 | 0.72–1.01 | 0.469 | 6.59 | 30.5 | 43.0 |
| rtt116-loss0.5 | chunked | h1 | 4 | 0/3 | 1.67 | 1.63–2.12 | 0.518 | 5.80 | 32.2 | 91.9 |
| rtt116-loss0.5 | chunked | h1 | 8 | 0/3 | 3.20 | 3.06–3.26 | 0.546 | 5.66 | 34.4 | 177.6 |
| rtt116-loss0.5 | chunked | h2 | 8 | 0/3 | 0.45 | 0.31–0.77 | 0.461 | 7.28 | 36.0 | 24.0 |

### rtt116-loss0.1-100mbit — RTT 116 ms, perda 0,1 % por sentido, upload limitado a 100 Mbit/s

Runner: AMD EPYC 9V74 80-Core Processor · 4 vCPU · kernel 7.0.0-1020-azure · netem `delay 58ms loss 0.1% rate 100mbit`

| variante | modo | proto | conexões | rodadas completas | vazão MB/s (mediana) | min–máx | retransmissões % | CPU servidor ms/MB | RAM servidor pico MiB | disco escrito MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| rtt116-loss0.1-100mbit+h2win16 | legacy | h2 | 1 | 0/3 | 1.35 | 1.27–2.00 | 0.089 | 18.90 | 31.4 | 74.5 |
| rtt116-loss0.1-100mbit+h2win16 | chunked | h2 | 1 | 0/3 | 1.70 | 1.34–2.18 | 0.115 | 9.81 | 33.5 | 104.3 |
| rtt116-loss0.1-100mbit+h2win16 | chunked | h2 | 8 | 0/3 | 2.48 | 1.77–2.52 | 0.082 | 8.20 | 35.5 | 154.5 |
| rtt116-loss0.1-100mbit+ssai0 | chunked | h1 | 1 | 0/3 | 1.51 | 1.28–2.22 | 0.121 | 9.68 | 29.0 | 83.2 |
| rtt116-loss0.1-100mbit | legacy | h1 | 1 | 0/3 | 1.44 | 0.97–2.37 | 0.088 | 11.21 | 27.9 | 82.3 |
| rtt116-loss0.1-100mbit | legacy | h2 | 1 | 0/3 | 1.23 | 1.19–1.28 | 0.125 | 19.57 | 31.4 | 69.7 |
| rtt116-loss0.1-100mbit | chunked | h1 | 1 | 0/3 | 1.55 | 0.89–2.11 | 0.116 | 9.92 | 32.0 | 87.1 |
| rtt116-loss0.1-100mbit | chunked | h1 | 2 | 0/3 | 2.47 | 2.25–2.53 | 0.121 | 10.02 | 31.7 | 138.5 |
| rtt116-loss0.1-100mbit | chunked | h1 | 4 | 2/3 | 4.02 | 3.35–4.97 | 0.112 | 9.84 | 33.3 | 192.1 |
| rtt116-loss0.1-100mbit | chunked | h1 | 8 | 3/3 | 6.00 | 4.90–7.28 | 0.105 | 10.23 | 35.4 | 192.1 |
| rtt116-loss0.1-100mbit | chunked | h2 | 8 | 0/3 | 1.62 | 1.24–2.19 | 0.106 | 9.02 | 37.2 | 90.8 |


**Conclusions (final run: 3 runs per configuration, median; see the tables for variation):** (1) HTTP/2 caps one upload at window ÷ RTT: at 116 ms without loss the legacy upload over h2 reached **8.4 MB/s** vs 23.3 MB/s over HTTP/1.1; with `http2_receive_window_mib: 16` it reached **23.8 MB/s**; chunked with 8 requests over h2 also stayed at 8.4 MB/s (25.7 with the 16 MiB window) because parallel requests share **one** TCP connection and window. (2) Separate HTTP/1.1 connections add up: 19.8 / 33.4 / 47.7 / 51.4 MB/s with 1/2/4/8 connections (no loss), 0.37 / 0.78 / 1.67 / 3.20 MB/s at 0.5 % loss, 1.55 / 2.47 / 4.02 / 6.00 MB/s at 0.1 % loss capped at 100 Mbit/s. (3) At 0.1 % loss run-to-run variation is large (2 connections: 14.0–28.7 MB/s), so no reliable monotonic scaling between 2, 4 and 8 is claimed; what the data supports is that every 2+-connection HTTP/1.1 configuration (11.9–22.0 MB/s) was ≥3.6× above the 1-connection ones (1.2–3.3 MB/s) and the only ones that finished 192 MiB in 60 s. (4) One chunked connection is ~15 % slower than a single POST (19.8 vs 23.3 MB/s), consistent with the response gap between consecutive chunks (not proven); the slow-start-after-idle hypothesis was tested and refuted. (5) Node cost: ~2.5–3.7 ms CPU/MB unimpaired, 5–10 ms/MB on slow profiles (noisy shared runners); peak RSS 29–36 MiB on HTTP/1.1, up to 52 MiB with 8 h2 streams and a 16 MiB window; disk writes equal the file size (no amplification). (6) Without emulation everything sits at ~120–147 MB/s (runner CPU limit). **Not established:** the gain on the real Brazil → node1 path, browser behaviour, or the panel's adaptive algorithm; the operational recommendation is to test `disable_http2` / a larger h2 window on staging over the real path before deciding.


## 4. Limitações do que foi medido

* Emulação de atraso/perda **uniforme e aleatória**; a internet tem perda em rajadas, variação de atraso e filas.
* O cliente do benchmark é Go; um **navegador** tem seu próprio TCP, limite de 6 conexões HTTP/1.1 por host e negocia HTTP/2 se o servidor oferecer.
* Execuções limitadas a 60 s por configuração; arquivos de 192 MiB; 3 rodadas (variação indicada por min–máx).
* A vazão do **algoritmo adaptativo do painel** não foi medida aqui (o cliente do benchmark usa um número fixo de conexões); ele foi validado por simulação nos testes do painel.
* Nenhuma medição em produção; nenhuma com o caminho real Brasil → node1.

---

## English

Additive build on **Wings v1.13.4**: the legacy upload, SFTP and all other endpoints are untouched. See the table in §1 for what was added (`server/uploadsession`, router handlers, JWT payload, filesystem primitives `OpenForChunkedWrite` / `ReplaceFile` / `RenameReplace`, `api.upload_sessions.*` config, benchmark tool and workflows). Tests: 18 session-logic test functions (Windows/Linux, `-race`), 11 HTTP integration tests on the real router and a real volume, and the whole upstream suite on linux amd64/arm64 in CI.

**Benchmark** (§3): an *emulated* network (veth + netns + `tc netem`, delay and loss in both directions, MTU 1500, offloads off) on shared GitHub runners; the real router in one process, a Go client in another; 192 MiB files, 60 s time box, 3 runs. Throughput is measured at the client; CPU/RAM/disk are the server process's; retransmissions are the client TCP's. **The numbers describe this emulation, not the real internet path**, and CPU/disk figures are noisy on shared hardware. Limitations (§4): uniform random loss, Go client rather than a browser, short runs, the panel's adaptive algorithm not measured here (validated by simulation), nothing measured in production or on the real Brazil → node1 path.
