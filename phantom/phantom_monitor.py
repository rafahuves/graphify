#!/usr/bin/env python3
"""
Monitor de SINAIS para contratos de 5 minutos "sobe/desce" (UP/DOWN).

NAO opera, NAO conecta carteira, NAO pede chave privada, seed ou login.
Usa apenas dados publicos:
  - Binance (BTC, ETH, SOL, XRP)  -> velas de 1 minuto, sem chave
  - Hyperliquid (HYPE)            -> velas de 1 minuto, sem chave

Comandos:
  python3 phantom_monitor.py rodar        # roda o monitor (Ctrl+C para parar)
  python3 phantom_monitor.py relatorio    # taxa de acerto por moeda
  python3 phantom_monitor.py testar       # testa APIs e notificacoes
  python3 phantom_monitor.py ntfy NOME    # liga envio para o topico ntfy NOME
  python3 phantom_monitor.py ntfy --desligar

Uso recomendado no Mac:
  cd ~/phantom && caffeinate -i python3 phantom_monitor.py rodar

Somente biblioteca padrao do Python 3 (nao precisa instalar nada).
"""
from __future__ import annotations

import argparse
import csv
import fcntl
import json
import math
import os
import platform
import random
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

# ----------------------------------------------------------------------------
# Configuracao
# ----------------------------------------------------------------------------
PASTA = Path(__file__).resolve().parent
ARQ_CSV = PASTA / "sinais.csv"
ARQ_LOCK = PASTA / ".phantom_monitor.lock"
ARQ_CONFIG = PASTA / "config.json"

# moeda -> (fonte, simbolo na fonte)
MOEDAS = {
    "BTC": ("binance", "BTCUSDT"),
    "ETH": ("binance", "ETHUSDT"),
    "SOL": ("binance", "SOLUSDT"),
    "XRP": ("binance", "XRPUSDT"),
    "HYPE": ("hyperliquid", "HYPE"),
}

JANELA_S = 300              # contratos de 5 minutos
MOMENTO_SINAL_S = 150       # 2min30s depois da abertura = faltam 2min30s
TOLERANCIA_ATRASO_S = 15    # passou disso, o sinal e descartado (atrasado)
PROB_MINIMA = 0.55          # so sinaliza com probabilidade >= 55%
VELAS_VOLATILIDADE = 60     # usa as ultimas 60 velas de 1 min fechadas
MIN_RETORNOS = 20           # minimo de retornos validos para estimar vol.
MC_CAMINHOS = 10000         # caminhos simulados no Monte Carlo
MC_PASSO_S = 10             # passo da simulacao (segundos)
ESPERA_FECHAMENTO_S = 5     # espera apos o fim da janela antes de apurar
INTERVALO_APURACAO_S = 15   # de quanto em quanto tempo tenta apurar pendentes
TIMEOUT_HTTP_S = 6

BINANCE_BASES = [
    "https://api.binance.com",
    "https://data-api.binance.vision",  # espelho publico (dados de mercado)
]
HYPERLIQUID_URL = "https://api.hyperliquid.xyz/info"
NTFY_BASE = "https://ntfy.sh"

COLUNAS_CSV = [
    "id", "criado_em", "moeda", "janela_inicio", "janela_inicio_epoch",
    "lado", "probabilidade", "preco_abertura", "preco_sinal",
    "volatilidade_1m", "preco_fechamento", "resultado", "apurado_em",
]

Vela = namedtuple("Vela", "t o h l c")  # t = abertura da vela em ms

_binance_base_ok = None  # lembra qual endereco da Binance funcionou


# ----------------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------------
def hora(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time()).strftime("%H:%M:%S")


def data_hora(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time()).strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{hora()}] {msg}", flush=True)


def fmt_preco(p: float) -> str:
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6f}"


def http_json(url: str, corpo: dict | None = None):
    """GET (corpo=None) ou POST JSON. Retorna o JSON decodificado."""
    dados = None
    cab = {"User-Agent": "phantom-monitor/1.0", "Accept": "application/json"}
    if corpo is not None:
        dados = json.dumps(corpo).encode()
        cab["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=dados, headers=cab)
    with urllib.request.urlopen(req, timeout=TIMEOUT_HTTP_S) as r:
        return json.loads(r.read().decode())


def binance_get(caminho: str, params: dict):
    global _binance_base_ok
    bases = [_binance_base_ok] if _binance_base_ok else []
    bases += [b for b in BINANCE_BASES if b != _binance_base_ok]
    ultimo_erro = None
    for base in bases:
        url = f"{base}{caminho}?{urllib.parse.urlencode(params)}"
        try:
            res = http_json(url)
            _binance_base_ok = base
            return res
        except urllib.error.HTTPError as e:
            ultimo_erro = e
            if e.code in (403, 451):  # regiao bloqueada -> tenta o espelho
                continue
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            ultimo_erro = e
            continue
    raise ultimo_erro if ultimo_erro else RuntimeError("Binance indisponivel")


# ----------------------------------------------------------------------------
# Velas (com validacao)
# ----------------------------------------------------------------------------
def _validar_velas(brutas, inicio_ms: int, fim_ms: int) -> list[Vela]:
    """Descarta velas invalidas, duplicadas ou fora do intervalo pedido."""
    por_t: dict[int, Vela] = {}
    for t, o, h, l, c in brutas:
        try:
            t = int(t)
            o, h, l, c = float(o), float(h), float(l), float(c)
        except (TypeError, ValueError):
            continue
        valores = (o, h, l, c)
        if t % 60000 != 0 or not (inicio_ms <= t <= fim_ms):
            continue
        if not all(math.isfinite(x) and x > 0 for x in valores):
            continue
        tol = 1e-9 * max(valores)
        if h + tol < max(o, c, l) or l - tol > min(o, c, h):
            continue
        por_t[t] = Vela(t, o, h, l, c)
    return [por_t[t] for t in sorted(por_t)]


def baixar_velas(moeda: str, inicio_ms: int, fim_ms: int) -> list[Vela]:
    """Velas de 1 min cuja abertura esta entre inicio_ms e fim_ms."""
    fonte, simbolo = MOEDAS[moeda]
    if fonte == "binance":
        dados = binance_get("/api/v3/klines", {
            "symbol": simbolo, "interval": "1m",
            "startTime": inicio_ms, "endTime": fim_ms, "limit": 1000,
        })
        brutas = [(k[0], k[1], k[2], k[3], k[4]) for k in dados]
    else:
        dados = http_json(HYPERLIQUID_URL, {
            "type": "candleSnapshot",
            "req": {"coin": simbolo, "interval": "1m",
                    "startTime": inicio_ms, "endTime": fim_ms},
        })
        brutas = [(k.get("t"), k.get("o"), k.get("h"), k.get("l"), k.get("c"))
                  for k in dados if isinstance(k, dict)]
    return _validar_velas(brutas, inicio_ms, fim_ms)


# ----------------------------------------------------------------------------
# Monte Carlo
# ----------------------------------------------------------------------------
def volatilidade_1m(velas_fechadas: list[Vela]) -> tuple[float, int]:
    """Desvio-padrao dos retornos log de 1 min (so pares consecutivos)."""
    rets = []
    for a, b in zip(velas_fechadas, velas_fechadas[1:]):
        if b.t - a.t == 60000:
            rets.append(math.log(b.c / a.c))
    if len(rets) < MIN_RETORNOS:
        return 0.0, len(rets)
    return statistics.stdev(rets), len(rets)


def monte_carlo(preco_atual: float, preco_abertura: float, sigma_1m: float,
                segundos_restantes: float, caminhos: int = MC_CAMINHOS,
                rng: random.Random | None = None) -> tuple[float, float]:
    """
    Simula caminhos de preco (passeio aleatorio log-normal, sem tendencia)
    ate o fechamento. Retorna (prob_UP, prob_DOWN).
    """
    rng = rng or random.Random()
    passos = max(1, round(segundos_restantes / MC_PASSO_S))
    desvio_passo = sigma_1m * math.sqrt((segundos_restantes / 60.0) / passos)
    alvo = math.log(preco_abertura / preco_atual)  # quanto falta para empatar
    gauss = rng.gauss
    acima = abaixo = 0
    for _ in range(caminhos):
        x = 0.0
        for _ in range(passos):
            x += gauss(0.0, desvio_passo)
        if x > alvo:
            acima += 1
        elif x < alvo:
            abaixo += 1
    return acima / caminhos, abaixo / caminhos


# ----------------------------------------------------------------------------
# CSV
# ----------------------------------------------------------------------------
def carregar_csv() -> list[dict]:
    if not ARQ_CSV.exists():
        return []
    with open(ARQ_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def salvar_csv(linhas: list[dict]) -> None:
    """Grava de forma atomica (arquivo temporario + rename)."""
    tmp = ARQ_CSV.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_CSV, extrasaction="ignore")
        w.writeheader()
        for linha in linhas:
            w.writerow({c: linha.get(c, "") for c in COLUNAS_CSV})
    os.replace(tmp, ARQ_CSV)


def adicionar_sinal(linha: dict) -> bool:
    """Adiciona o sinal se o id ainda nao existir. Retorna True se gravou."""
    linhas = carregar_csv()
    if any(l.get("id") == linha["id"] for l in linhas):
        return False
    linhas.append(linha)
    salvar_csv(linhas)
    return True


# ----------------------------------------------------------------------------
# Configuracao do ntfy
# ----------------------------------------------------------------------------
def ler_config() -> dict:
    try:
        return json.loads(ARQ_CONFIG.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def topico_ntfy() -> str | None:
    t = os.environ.get("PHANTOM_NTFY_TOPICO") or ler_config().get("ntfy_topico")
    return t if t and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", t) else None


# ----------------------------------------------------------------------------
# Notificacoes
# ----------------------------------------------------------------------------
def notificar_mac(titulo: str, mensagem: str) -> None:
    if platform.system() != "Darwin":
        return
    # texto passado como argumento (nao interpolado no script) -> seguro
    script = [
        "-e", "on run argv",
        "-e", 'display notification (item 2 of argv) with title (item 1 of argv) sound name "Glass"',
        "-e", "end run",
    ]
    try:
        subprocess.run(["osascript", *script, titulo, mensagem],
                       timeout=10, capture_output=True)
    except (OSError, subprocess.SubprocessError) as e:
        log(f"Aviso: notificacao do macOS falhou: {e}")


def notificar_ntfy(titulo: str, mensagem: str, tags: str = "chart_with_upwards_trend") -> None:
    topico = topico_ntfy()
    if not topico:
        return
    req = urllib.request.Request(
        f"{NTFY_BASE}/{topico}", data=mensagem.encode("utf-8"),
        headers={"Title": titulo.encode("ascii", "ignore").decode(),
                 "Tags": tags, "Priority": "high",
                 "User-Agent": "phantom-monitor/1.0"},
        method="POST")
    try:
        urllib.request.urlopen(req, timeout=TIMEOUT_HTTP_S).read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log(f"Aviso: envio para o ntfy falhou: {e}")


# ----------------------------------------------------------------------------
# Lock (uma copia por vez)
# ----------------------------------------------------------------------------
def pegar_lock():
    arq = open(ARQ_LOCK, "a+")
    try:
        fcntl.flock(arq, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Ja existe uma copia do monitor rodando. Feche-a antes (Ctrl+C na outra janela).")
        sys.exit(1)
    arq.seek(0)
    arq.truncate()
    arq.write(str(os.getpid()))
    arq.flush()
    return arq  # manter aberto enquanto o programa roda


# ----------------------------------------------------------------------------
# Logica principal
# ----------------------------------------------------------------------------
def preco_abertura(moeda: str, inicio_janela: int) -> float | None:
    ini_ms = inicio_janela * 1000
    velas = baixar_velas(moeda, ini_ms, ini_ms)
    return velas[0].o if velas and velas[0].t == ini_ms else None


def analisar(moeda: str, inicio_janela: int, abertura_guardada: float | None) -> dict:
    """Busca velas, valida e calcula a probabilidade. Sempre retorna um dict."""
    agora = time.time()
    agora_ms = int(agora * 1000)
    minuto_atual_ms = (agora_ms // 60000) * 60000
    ini_ms = inicio_janela * 1000
    res = {"moeda": moeda, "ok": False}

    velas = baixar_velas(moeda, ini_ms - (VELAS_VOLATILIDADE + 5) * 60000, agora_ms)
    por_t = {v.t: v for v in velas}

    vela_abre = por_t.get(ini_ms)
    abertura = abertura_guardada or (vela_abre.o if vela_abre else None)
    if not abertura:
        res["motivo"] = "sem preco de abertura da janela"
        return res

    vela_atual = por_t.get(minuto_atual_ms)
    if vela_atual is None:
        res["motivo"] = "vela do minuto atual ausente (dados atrasados)"
        return res

    fechadas = [v for v in velas if v.t + 60000 <= minuto_atual_ms]
    fechadas = fechadas[-(VELAS_VOLATILIDADE + 1):]
    sigma, n = volatilidade_1m(fechadas)
    if sigma <= 0:
        res["motivo"] = f"poucas velas validas para volatilidade ({n})"
        return res

    restante = inicio_janela + JANELA_S - time.time()
    if restante <= 0:
        res["motivo"] = "janela ja fechou"
        return res

    p_up, p_down = monte_carlo(vela_atual.c, abertura, sigma, restante)
    res.update(ok=True, abertura=abertura, atual=vela_atual.c, sigma=sigma,
               p_up=p_up, p_down=p_down)
    return res


def gerar_sinais(inicio_janela: int, aberturas: dict, ids_existentes: set,
                 prob_minima: float) -> None:
    momento = inicio_janela + MOMENTO_SINAL_S
    with ThreadPoolExecutor(max_workers=len(MOEDAS)) as ex:
        futuros = {m: ex.submit(analisar, m, inicio_janela, aberturas.get((m, inicio_janela)))
                   for m in MOEDAS}
    resultados = []
    for m, fut in futuros.items():
        try:
            resultados.append(fut.result())
        except Exception as e:  # rede, JSON etc.
            resultados.append({"moeda": m, "ok": False, "motivo": f"erro ao baixar dados: {e}"})

    atraso = time.time() - momento
    if atraso > TOLERANCIA_ATRASO_S:
        log(f"Calculo terminou {atraso:.0f}s atrasado -> nenhum sinal enviado nesta janela.")
        return

    fim = datetime.fromtimestamp(inicio_janela + JANELA_S).strftime("%H:%M")
    log(f"--- Janela {hora(inicio_janela)[:5]}-{fim} (faltam ~2min30s) ---")
    for r in resultados:
        m = r["moeda"]
        if not r["ok"]:
            log(f"  {m:<4} sem analise: {r['motivo']}")
            continue
        lado, prob = ("UP", r["p_up"]) if r["p_up"] >= r["p_down"] else ("DOWN", r["p_down"])
        resumo = (f"{m:<4} {lado:<4} {prob*100:5.1f}%  abertura {fmt_preco(r['abertura'])}"
                  f"  atual {fmt_preco(r['atual'])}")
        if prob < prob_minima:
            log(f"  {resumo}  (abaixo de {prob_minima*100:.0f}%, sem sinal)")
            continue

        sid = f"{m}-{inicio_janela}"
        if sid in ids_existentes:
            continue
        linha = {
            "id": sid, "criado_em": data_hora(), "moeda": m,
            "janela_inicio": data_hora(inicio_janela),
            "janela_inicio_epoch": str(inicio_janela), "lado": lado,
            "probabilidade": f"{prob:.4f}", "preco_abertura": repr(r["abertura"]),
            "preco_sinal": repr(r["atual"]), "volatilidade_1m": f"{r['sigma']:.6g}",
            "preco_fechamento": "", "resultado": "", "apurado_em": "",
        }
        if not adicionar_sinal(linha):
            ids_existentes.add(sid)
            continue
        ids_existentes.add(sid)
        seta = "▲" if lado == "UP" else "▼"
        print(f"\n  >>> SINAL {seta} {resumo}  (fecha {fim})\n", flush=True)
        titulo = f"{m} {lado} {prob*100:.0f}%"
        msg = (f"{m} {lado} {prob*100:.1f}% | abertura {fmt_preco(r['abertura'])} | "
               f"atual {fmt_preco(r['atual'])} | fecha {fim}")
        notificar_mac(titulo, msg)
        notificar_ntfy(titulo, msg, "arrow_up" if lado == "UP" else "arrow_down")


def apurar_pendentes() -> None:
    """Para cada sinal sem resultado cuja janela ja fechou, grava o resultado."""
    linhas = carregar_csv()
    agora = time.time()
    mudou = False
    for l in linhas:
        if l.get("resultado"):
            continue
        try:
            ini = int(l["janela_inicio_epoch"])
            abertura = float(l["preco_abertura"])
        except (KeyError, ValueError):
            continue
        if l.get("moeda") not in MOEDAS:
            continue
        if agora < ini + JANELA_S + ESPERA_FECHAMENTO_S:
            continue
        ultimo_ms = (ini + JANELA_S - 60) * 1000  # ultima vela de 1 min da janela
        try:
            velas = baixar_velas(l["moeda"], ultimo_ms, ultimo_ms)
        except Exception as e:
            log(f"Ainda nao deu para apurar {l['id']}: {e}")
            continue
        if not velas or velas[0].t != ultimo_ms:
            continue  # vela ainda nao disponivel; tenta de novo depois
        fech = velas[0].c
        if fech == abertura:
            resultado = "empate"
        elif (fech > abertura) == (l["lado"] == "UP"):
            resultado = "acerto"
        else:
            resultado = "erro"
        l.update(preco_fechamento=repr(fech), resultado=resultado, apurado_em=data_hora())
        mudou = True
        icone = {"acerto": "✅", "erro": "❌", "empate": "➖"}[resultado]
        log(f"{icone} {l['moeda']} {l['lado']} ({l['janela_inicio'][11:16]}): {resultado.upper()}"
            f"  abertura {fmt_preco(abertura)}  fechamento {fmt_preco(fech)}")
    if mudou:
        salvar_csv(linhas)


def rodar(args) -> None:
    lock = pegar_lock()  # noqa: F841  (precisa continuar aberto)
    prob_minima = args.prob
    ids = {l["id"] for l in carregar_csv() if l.get("id")}
    aberturas: dict = {}
    janelas_abertas: set = set()
    janelas_decididas: set = set()
    prox_apuracao = 0.0

    log("Monitor de sinais iniciado (somente dados publicos, nao opera).")
    log(f"Moedas: {', '.join(MOEDAS)} | sinal com prob >= {prob_minima*100:.0f}% | CSV: {ARQ_CSV}")
    log(f"ntfy: {'topico ' + topico_ntfy() if topico_ntfy() else 'desligado'} | Ctrl+C para parar.")

    agora = time.time()
    ini = int(agora // JANELA_S) * JANELA_S
    if agora - ini > MOMENTO_SINAL_S + TOLERANCIA_ATRASO_S:
        janelas_decididas.add(ini)  # janela atual ja passou do ponto: espera a proxima
        log(f"Janela atual ja passou do ponto de sinal. Primeira analise as "
            f"{hora(ini + JANELA_S + MOMENTO_SINAL_S)}.")
    else:
        log(f"Primeira analise as {hora(ini + MOMENTO_SINAL_S)}.")

    try:
        while True:
            agora = time.time()
            ini = int(agora // JANELA_S) * JANELA_S
            decorrido = agora - ini

            # 1-2) guarda o preco de abertura logo apos a janela abrir
            if ini not in janelas_abertas and 3 <= decorrido < MOMENTO_SINAL_S:
                janelas_abertas.add(ini)
                for m in MOEDAS:
                    try:
                        p = preco_abertura(m, ini)
                        if p:
                            aberturas[(m, ini)] = p
                    except Exception as e:
                        log(f"  {m}: nao consegui a abertura agora ({e}); tento no calculo.")

            # 3-5) calcula e sinaliza quando faltar 2min30s
            if ini not in janelas_decididas and decorrido >= MOMENTO_SINAL_S:
                janelas_decididas.add(ini)
                if decorrido <= MOMENTO_SINAL_S + TOLERANCIA_ATRASO_S:
                    gerar_sinais(ini, aberturas, ids, prob_minima)
                else:
                    log(f"Janela {hora(ini)[:5]}: passou do horario do sinal "
                        f"(o Mac dormiu ou travou?) -> sem sinal.")

            # 6) apura resultados de janelas fechadas
            if agora >= prox_apuracao:
                prox_apuracao = agora + INTERVALO_APURACAO_S
                try:
                    apurar_pendentes()
                except Exception as e:
                    log(f"Erro na apuracao: {e}")

            # limpeza de memoria
            for k in [k for k in aberturas if k[1] < ini - JANELA_S]:
                del aberturas[k]
            janelas_abertas = {j for j in janelas_abertas if j >= ini - JANELA_S}
            janelas_decididas = {j for j in janelas_decididas if j >= ini - JANELA_S}

            time.sleep(0.5)
    except KeyboardInterrupt:
        print()
        log("Monitor encerrado.")


# ----------------------------------------------------------------------------
# Relatorio
# ----------------------------------------------------------------------------
def relatorio(_args) -> None:
    linhas = carregar_csv()
    if not linhas:
        print("Nenhum sinal gravado ainda. Rode o monitor primeiro.")
        return
    stats: dict = {}
    for l in linhas:
        s = stats.setdefault(l.get("moeda", "?"),
                             {"sinais": 0, "acerto": 0, "erro": 0, "empate": 0, "pend": 0, "prob": 0.0})
        s["sinais"] += 1
        res = l.get("resultado")
        if res in ("acerto", "erro", "empate"):
            s[res] += 1
            try:
                s["prob"] += float(l["probabilidade"])
            except (KeyError, ValueError):
                pass
        else:
            s["pend"] += 1

    cab = f"{'Moeda':<6}{'Sinais':>7}{'Acertos':>9}{'Erros':>7}{'Empates':>9}{'Pend.':>7}{'Acerto%':>9}{'Prev.%':>8}{'± margem':>10}"
    print("\nRELATORIO DE SINAIS  (acerto% = acertos / apurados; empate conta como nao-acerto)")
    print(cab)
    print("-" * len(cab))
    total = {"sinais": 0, "acerto": 0, "erro": 0, "empate": 0, "pend": 0, "prob": 0.0}
    for moeda in sorted(stats, key=lambda m: list(MOEDAS).index(m) if m in MOEDAS else 99):
        s = stats[moeda]
        for k in total:
            total[k] += s[k]
        _linha_relatorio(moeda, s)
    print("-" * len(cab))
    _linha_relatorio("TOTAL", total)
    print("\nPrev.% = probabilidade media que o modelo previu. Se Acerto% ficar bem abaixo,")
    print("o modelo esta otimista. '± margem' e o erro aproximado (95%): com poucos sinais")
    print("a taxa de acerto ainda nao significa quase nada.")
    print("\nLEMBRE: para lucrar, a taxa de acerto precisa superar o PRECO que voce paga por")
    print("contrato (ex.: pagar 0,60 para receber 1,00 exige acertar mais de 60%, + taxas).\n")


def _linha_relatorio(nome: str, s: dict) -> None:
    n = s["acerto"] + s["erro"] + s["empate"]
    if n:
        taxa = s["acerto"] / n
        margem = 1.96 * math.sqrt(0.25 / n)  # margem de pior caso (95%)
        txt = f"{taxa*100:8.1f}%{s['prob']/n*100:7.1f}%{margem*100:8.1f}pp"
    else:
        txt = f"{'-':>9}{'-':>8}{'-':>10}"
    print(f"{nome:<6}{s['sinais']:>7}{s['acerto']:>9}{s['erro']:>7}{s['empate']:>9}{s['pend']:>7}{txt}")


# ----------------------------------------------------------------------------
# Testar / ntfy
# ----------------------------------------------------------------------------
def testar(_args) -> None:
    print("Testando conexoes (somente leitura de dados publicos)...\n")
    try:
        srv = binance_get("/api/v3/time", {})["serverTime"] / 1000
        dif = time.time() - srv
        print(f"Relogio do Mac vs Binance: diferenca de {dif:+.1f}s"
              + ("  <-- ATENCAO: ajuste o relogio (Ajustes > Geral > Data e Hora)" if abs(dif) > 2 else "  (ok)"))
        print(f"Endereco Binance em uso: {_binance_base_ok}")
    except Exception as e:
        print(f"Binance: FALHOU ({e})")
    agora_ms = int(time.time() * 1000)
    for m in MOEDAS:
        try:
            velas = baixar_velas(m, agora_ms - 10 * 60000, agora_ms)
            if not velas:
                print(f"  {m:<4} sem velas retornadas")
                continue
            idade = (agora_ms - velas[-1].t) / 1000
            print(f"  {m:<4} OK  preco {fmt_preco(velas[-1].c):>12}  ({len(velas)} velas, "
                  f"ultima aberta ha {idade:.0f}s)")
        except Exception as e:
            print(f"  {m:<4} FALHOU: {e}")
    print("\nEnviando notificacao de teste...")
    notificar_mac("Phantom monitor", "Teste de notificacao OK")
    if topico_ntfy():
        notificar_ntfy("Phantom monitor", "Teste de notificacao OK", "white_check_mark")
        print(f"Enviado tambem para o ntfy (topico {topico_ntfy()}).")
    else:
        print("ntfy desligado (use: python3 phantom_monitor.py ntfy NOME-DO-TOPICO).")


def configurar_ntfy(args) -> None:
    cfg = ler_config()
    if args.desligar or not args.topico:
        cfg.pop("ntfy_topico", None)
        print("Envio para o ntfy DESLIGADO.")
    else:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.topico):
            print("Nome invalido. Use so letras, numeros, - e _ (ate 64 caracteres).")
            sys.exit(1)
        cfg["ntfy_topico"] = args.topico
        print(f"Envio para o ntfy LIGADO no topico '{args.topico}'.")
        print("No app ntfy do celular, assine exatamente esse topico.")
    ARQ_CONFIG.write_text(json.dumps(cfg, indent=2), encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description="Monitor de sinais UP/DOWN 5 min (nao opera).")
    sub = p.add_subparsers(dest="comando", required=True)
    pr = sub.add_parser("rodar", help="roda o monitor")
    pr.add_argument("--prob", type=float, default=PROB_MINIMA,
                    help="probabilidade minima para sinal (padrao 0.55)")
    pr.set_defaults(func=rodar)
    sub.add_parser("relatorio", help="taxa de acerto por moeda").set_defaults(func=relatorio)
    sub.add_parser("testar", help="testa APIs e notificacoes").set_defaults(func=testar)
    pn = sub.add_parser("ntfy", help="liga/desliga envio para o app ntfy")
    pn.add_argument("topico", nargs="?")
    pn.add_argument("--desligar", action="store_true")
    pn.set_defaults(func=configurar_ntfy)
    args = p.parse_args()
    if getattr(args, "prob", 0.5) < 0.5 or getattr(args, "prob", 0.5) >= 1:
        p.error("--prob deve estar entre 0.5 e 1")
    args.func(args)


if __name__ == "__main__":
    main()
