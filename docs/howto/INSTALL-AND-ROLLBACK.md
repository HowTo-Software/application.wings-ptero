# Instalação, verificação e reversão (homologação) / Install, verify, roll back (staging)

> **Não aplicar em nodes de produção sem autorização.** Este procedimento é para um node de **homologação**.
> Nada aqui foi executado em produção. / **Do not apply to production nodes without approval.** Staging only.

Português primeiro; English below.

## 0. Pré-requisitos

* Um node de homologação com Wings **v1.13.4** (confirme: `wings version`) e acesso **root** ao host.
* Um servidor de teste nesse node (não use servidores de clientes).
* O painel com o PR de upload fragmentado (`application.panel-ptero`) implantado **no ambiente de homologação**, ou um painel de teste.
* Janela de manutenção: o reinício do Wings **derruba WebSocket e SFTP por alguns segundos** (containers de jogo continuam rodando).

## 1. Obter e conferir o binário

O CI (`HowTo CI`) publica o artefato `wings-howto_linux_amd64` (e `arm64`) com `wings.sha256`.

```bash
gh run download <run-id> --repo HowTo-Software/application.wings-ptero -n wings-howto_linux_amd64 -D /tmp/wings-howto
cd /tmp/wings-howto && sha256sum -c wings.sha256          # tem que dar "OK"
./wings version                                           # v1.13.4-howto.<sha>
```

## 2. Backup do binário e da configuração atuais (OBRIGATÓRIO antes de trocar)

```bash
sudo cp -a /usr/local/bin/wings /usr/local/bin/wings.upstream-v1.13.4
sudo sha256sum /usr/local/bin/wings.upstream-v1.13.4 | sudo tee /usr/local/bin/wings.upstream-v1.13.4.sha256
sudo cp -a /etc/pterodactyl/config.yml /etc/pterodactyl/config.yml.bak-before-upload-sessions
```

## 3. Instalar

```bash
sudo systemctl stop wings
sudo install -m 755 /tmp/wings-howto/wings /usr/local/bin/wings
sudo systemctl start wings
sudo systemctl status wings --no-pager | head -5
/usr/local/bin/wings version                               # confirma o build com fork
```

**Configuração (opcional)** em `/etc/pterodactyl/config.yml`; sem o bloco, valem os padrões abaixo:

```yaml
api:
  upload_limit: 10240            # MiB; para aceitar 10 GiB (o painel NÃO altera este valor)
  upload_sessions:
    enabled: true                # false = o node deixa de anunciar e recusa as rotas novas
    chunk_size: 16777216         # 16 MiB (não afeta sessões já iniciadas)
    max_parallel_chunks: 8
    max_sessions_per_server: 4
    session_ttl_hours: 24        # sessão ociosa é apagada junto com o .part
    disable_http2: false         # true = só HTTP/1.1 no listener HTTPS (ver relatório de benchmark)
    http2_receive_window_mib: 0  # 0 = padrão do Go (1 MiB); ex. 16 (ver relatório de benchmark)
```

Depois de editar: `sudo systemctl restart wings`.

## 4. Verificar

```bash
# 1. o node anuncia a capacidade (use o token do node de homologação; NUNCA o cole em chat/PR)
curl -s -H "Authorization: Bearer $NODE_TOKEN" "https://<node>:8443/api/system?v=2" | jq '.capabilities'
#   { "howto_upload_sessions": { "version": 1, "chunk_size": 16777216, "max_parallel_chunks": 8, ... } }

# 2. upload legado continua funcionando (pelo painel antigo ou com um token de upload normal)

# 3. pelo painel de homologação: enviar ~200 MB e, durante o envio:
ls -la /var/lib/pterodactyl/volumes/<uuid>/<pasta>/        # .upload-<id>.part crescendo; o nome final NÃO existe ainda
ls /var/lib/pterodactyl/upload-sessions/<uuid>/            # <id>.json e <id>.chunks (fora do volume)
#   ao concluir: só o arquivo final, dono = usuário do servidor; sessão vira "lápide" por 1 h

# 4. retomar: iniciar o envio, FECHAR o navegador no meio, reabrir e selecionar o MESMO arquivo -> continua de onde parou
# 5. cancelar no painel -> .part removido e cota liberada
# 6. SFTP: conectar e listar/baixar/enviar um arquivo (não deve mudar nada)
sudo journalctl -u wings -n 50 --no-pager                  # sem erros novos
```

## 5. Reverter (a qualquer momento)

```bash
sudo systemctl stop wings
sudo cp -a /usr/local/bin/wings.upstream-v1.13.4 /usr/local/bin/wings
sudo sha256sum -c /usr/local/bin/wings.upstream-v1.13.4.sha256     # confirma que voltou ao binário original
# opcional: restaurar a configuração anterior (chaves desconhecidas do upstream são ignoradas, mas é mais limpo remover o bloco)
sudo cp -a /etc/pterodactyl/config.yml.bak-before-upload-sessions /etc/pterodactyl/config.yml
sudo systemctl start wings
```

**Limpeza depois de reverter** (o Wings original não conhece estes arquivos e não os apaga):

```bash
sudo rm -rf /var/lib/pterodactyl/upload-sessions                              # estado das sessões
sudo find /var/lib/pterodactyl/volumes -name '.upload-*.part' -print          # conferir primeiro
sudo find /var/lib/pterodactyl/volumes -name '.upload-*.part' -delete         # depois apagar (libera cota)
```

**Painel:** nenhum passo. Com o Wings original o painel deixa de ver `capabilities.howto_upload_sessions` (cache de 5 min) e volta ao upload regular sozinho.

## 6. Critérios para considerar a homologação aprovada

* `go test` verde no CI (inclui a suíte upstream e `-race`) e benchmark executado com o **caminho de rede real** de usuários (ver relatório);
* upload de 10 GiB concluído com SHA-256 conferido, retomado ao menos uma vez após fechar o navegador;
* SFTP e upload legado sem regressão; nenhum aumento anormal de RAM/CPU do Wings durante os testes;
* decisão registrada sobre `disable_http2` e `http2_receive_window_mib` **com base em medição**.

---

## English

**Staging only. Not executed in production.** Prerequisites: a staging node on Wings v1.13.4, root, a test server, the panel with the chunked-upload PR on a staging panel, and a maintenance window (restart drops WebSocket/SFTP for seconds; game containers keep running).

1. **Get and verify** the CI artifact `wings-howto_linux_amd64` (`sha256sum -c wings.sha256`, `wings version` shows `v1.13.4-howto.<sha>`).
2. **Back up** the current binary (`wings.upstream-v1.13.4` + sha256) and `config.yml` **before** replacing anything.
3. **Install**: stop, `install -m 755`, start. Optional `api.upload_sessions.*` block (defaults apply without it); `api.upload_limit: 10240` for 10 GiB.
4. **Verify**: `GET /api/system?v=2` shows `capabilities.howto_upload_sessions`; during an upload a hidden `.upload-<id>.part` grows and the final name does not exist yet; session state is under `upload-sessions/` outside the volume; close the browser mid-upload and select the same file again to resume; cancel frees the quota; SFTP and the legacy upload are unchanged.
5. **Roll back** any time: stop, restore the saved binary (verify the sha256), optionally restore the saved config, start. Then clean up `upload-sessions/` and leftover `.upload-*.part` files (upstream Wings does not know them). The panel needs no action: it stops seeing the capability (5 min cache) and falls back to the regular upload.
6. **Acceptance**: green CI + benchmark on the real user network path; a 10 GiB upload completed with verified SHA-256 and resumed at least once; no SFTP/legacy regression; no abnormal RAM/CPU; `disable_http2` / `http2_receive_window_mib` decided from measurements.
