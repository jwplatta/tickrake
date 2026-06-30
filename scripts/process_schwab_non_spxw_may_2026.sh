#!/usr/bin/env bash
set -euo pipefail

PROVIDER="${PROVIDER:-schwab}"
START_DATE="${START_DATE:-2026-05-01}"
END_DATE="${END_DATE:-2026-05-31}"
CONCURRENCY="${CONCURRENCY:-4}"

TICKERS=(
  AAPL
  ACN
  ADBE
  ADI
  ADP
  ADSK
  AMAT
  AMD
  AMZN
  ANET
  AVGO
  CDNS
  CRM
  CRWD
  CSCO
  DELL
  DIA
  DXC
  EPAM
  FICO
  FTNT
  GEN
  GOOG
  GPN
  HOOD
  HPE
  HPQ
  IBM
  INTC
  INTU
  IT
  IWM
  KLAC
  LRCX
  MA
  MCHP
  META
  MPWR
  MRVL
  MSCI
  MSFT
  MU
  NDX
  NDXP
  NFLX
  NOW
  NTAP
  NVDA
  NXPI
  ON
  ORCL
  PANW
  PAYC
  PAYX
  PLTR
  PYPL
  QCOM
  QQQ
  RSP
  RUT
  RUTW
  SBUX
  SCHW
  SNPS
  SPY
  STX
  SWKS
  TER
  TSLA
  TXN
  UBER
  V
  VIX
  VIXW
  WDC
  XLB
  XLC
  XLE
  XLF
  XLI
  XLK
  XLP
  XLRE
  XLU
  XLV
  XLY
  XSP
)

for ticker in "${TICKERS[@]}"; do
  echo
  echo "=== Processing ${ticker} (${START_DATE} to ${END_DATE}) ==="
  bundle exec ruby scripts/process_compacted_option_samples.rb \
    --provider "${PROVIDER}" \
    --ticker "${ticker}" \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --concurrency "${CONCURRENCY}"
done
