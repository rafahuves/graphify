# Monitor de sinais Phantom (5 min, sobe/desce)

Monitor de **sinais** para BTC, ETH, SOL, XRP e HYPE. Ele **não opera**, **não conecta
carteira** e **nunca pede chave privada, seed ou login**. Só lê dados públicos de mercado
(Binance e Hyperliquid). Usa apenas o Python que já vem no Mac, sem instalar pacotes.

> ⚠️ Nunca digite sua seed/frase de recuperação ou chave privada em nenhum script,
> site ou chat. Este programa não precisa disso para nada.

## Como funciona

| Momento da janela | O que o script faz |
|---|---|
| :00 (+3 s) | Guarda o preço de abertura (abertura da vela de 1 min que começa a janela) |
| 2min30s (faltando 2min30s) | Pega as últimas 60 velas de 1 min, mede a volatilidade e roda 10.000 simulações Monte Carlo do preço até o fechamento |
| Se a probabilidade de UP ou de DOWN for ≥ 55% | Mostra no Terminal, notifica no macOS, envia ao ntfy (se ligado) e grava em `sinais.csv` |
| Fim da janela (+5 s) | Lê o fechamento da última vela e grava `acerto`, `erro` ou `empate` |

Proteções: uma cópia por vez (arquivo de lock), sinal duplicado é bloqueado pelo id
`MOEDA-janela`, velas inválidas são descartadas, e o sinal é descartado se a vela do minuto
atual não tiver chegado (dados atrasados) ou se o cálculo terminar mais de 15 s depois
do horário (por exemplo, se o Mac dormiu).

## Passo a passo no Mac (iniciante)

**1. Abra o Terminal:** `Cmd + Espaço`, digite `Terminal`, Enter.

**2. Confira o Python:**
```bash
python3 --version
```
Se aparecer uma janela pedindo para instalar as "ferramentas de linha de comando",
clique em **Instalar**, espere terminar e rode o comando de novo.

**3. Crie a pasta:**
```bash
mkdir -p ~/phantom && cd ~/phantom
```

**4. Crie o arquivo do script.** Copie **todo** o conteúdo de `phantom_monitor.py`
(Cmd+A, Cmd+C) e rode:
```bash
pbpaste > ~/phantom/phantom_monitor.py
```
(`pbpaste` grava o que está na área de transferência. Não use o TextEdit, que troca as
aspas e quebra o código.) Confira:
```bash
head -3 ~/phantom/phantom_monitor.py
```
Deve começar com `#!/usr/bin/env python3`.

**5. Teste as conexões e a notificação:**
```bash
cd ~/phantom && python3 phantom_monitor.py testar
```
Todas as moedas devem aparecer com `OK`. Se a notificação não aparecer, abra
**Ajustes do Sistema → Notificações → Editor de Scripts** (ou "Script Editor") e permita.

**6. (Opcional) Celular com ntfy:**
1. Instale o app **ntfy** (iPhone ou Android).
2. Escolha um nome **difícil de adivinhar** (os tópicos do ntfy.sh são públicos: quem
   souber o nome lê as mensagens), por exemplo `sinais-rafa-8k2q9x`.
3. No app, toque em **+** e assine esse tópico.
4. No Terminal:
   ```bash
   cd ~/phantom && python3 phantom_monitor.py ntfy sinais-rafa-8k2q9x
   python3 phantom_monitor.py testar
   ```
   Para desligar: `python3 phantom_monitor.py ntfy --desligar`.

**7. Rode o monitor:**
```bash
cd ~/phantom && caffeinate -i python3 phantom_monitor.py rodar
```
`caffeinate -i` impede o Mac de dormir por inatividade enquanto o script roda. Deixe o
Mac **na tomada e com a tampa aberta** (com a tampa fechada ele dorme mesmo assim).
Para parar: `Ctrl + C`.

**8. Veja a taxa de acerto** (pode ser em outra janela do Terminal, com o monitor rodando):
```bash
cd ~/phantom && python3 phantom_monitor.py relatorio
```

Os sinais ficam em `~/phantom/sinais.csv`, que abre no Numbers ou no Excel.

## Problemas comuns

| Mensagem | Solução |
|---|---|
| `CERTIFICATE_VERIFY_FAILED` | Se você instalou o Python pelo python.org: abra Finder → Aplicativos → pasta Python 3.x → dê dois cliques em **Install Certificates.command** |
| `Ja existe uma copia do monitor rodando` | Já tem um monitor aberto em outra janela. Feche-o com Ctrl+C |
| `vela do minuto atual ausente` | A API atrasou; o script pula esse sinal de propósito |
| Binance com erro 451 | Bloqueio por região; o script já tenta o espelho público `data-api.binance.vision` |
| `passou do horario do sinal` | O Mac dormiu ou travou; essa janela é pulada |

Para mudar o limite de probabilidade: `python3 phantom_monitor.py rodar --prob 0.60`.

## Acerto alto não é lucro

- **O pagamento é pequeno.** Nesses contratos você paga um preço (por exemplo 0,60) para
  receber 1,00 se acertar. Acertando, ganha 0,40; errando, perde 0,60. Para empatar, a taxa
  de acerto precisa ser **maior que o preço pago, mais as taxas**. Quem paga 0,70 e acerta
  68% perde dinheiro, mesmo "acertando muito".
- **O mercado já sabe o que o modelo sabe.** Faltando 2min30s com o preço acima da abertura,
  o contrato UP já costuma estar caro. Um sinal de 60% só tem valor se o UP custar
  bem menos que 0,60.
- **A fonte de preço pode ser outra.** A Phantom pode resolver o contrato com outro preço
  (outro oráculo ou outra corretora), e a regra de empate pode ser diferente. Quando o preço
  está perto da abertura, uma diferença de centavos muda o resultado. Confira as regras do
  contrato.
- **O modelo é simples.** Ele supõe passeio aleatório sem tendência e volatilidade constante.
  Notícias e saltos bruscos não entram na conta. Compare `Acerto%` com `Prev.%` no relatório:
  se o acerto real ficar abaixo do previsto, o modelo está otimista.
- **Poucos sinais não provam nada.** Com 30 sinais a margem de erro passa de ±18 pontos.
  Junte centenas antes de tirar conclusões.

Isto é uma ferramenta de estudo, não recomendação de investimento.
