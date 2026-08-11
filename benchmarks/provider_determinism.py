"""Which OpenRouter providers actually return identical output twice at temp 0?"""
import os, sys, json, hashlib, time
import requests
from dotenv import load_dotenv
load_dotenv(r"d:\UC-2-Regulatory-Compliance-Library\UC-2-Regulatory-Compliance-Library\.env", override=True)

KEY = os.getenv("OPENROUTER_API_KEY")
MODEL = "deepseek/deepseek-v3.2"

# Long enough to give divergence room to show, short enough to stay cheap.
PROMPT = """Extract every binding obligation from the text below as minified JSON:
{"obligations":[{"id":"OB-001","text":""}]}

Text:
A bank must obtain a licence from SAMA before commencing business. The bank shall
maintain a deposit with SAMA equal to 15% of its deposit liabilities. Banks must not
grant loans exceeding 25% of reserves to any single borrower. Every bank shall submit
audited annual accounts to SAMA within three months of year end. A bank must not engage
in wholesale trade. The board shall appoint two auditors approved by SAMA. Banks are
required to publish their balance sheet in two local newspapers annually."""

PROVIDERS = ["AtlasCloud", "DeepInfra", "Novita", "SiliconFlow", "GMICloud", "Baidu"]
RUNS = 3


def call(provider, seed=12345):
    payload = {
        "model": MODEL,
        "temperature": 0,
        "top_p": 1,
        "seed": seed,
        "max_tokens": 900,
        "messages": [{"role": "user", "content": PROMPT}],
        "provider": {"order": [provider], "allow_fallbacks": False},
    }
    r = requests.post("https://openrouter.ai/api/v1/chat/completions",
                      headers={"Authorization": f"Bearer {KEY}",
                               "Content-Type": "application/json"},
                      json=payload, timeout=120)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}: {r.text[:120]}"
    b = r.json()
    ch = (b.get("choices") or [{}])[0]
    return (ch.get("message") or {}).get("content", ""), None


print(f"{RUNS} identical calls per provider, temperature=0, seed=12345\n")
results = {}
for prov in PROVIDERS:
    outs, err = [], None
    for i in range(RUNS):
        txt, e = call(prov)
        if e:
            err = e
            break
        outs.append(txt)
        time.sleep(0.5)
    if err:
        print(f"  {prov:<14} SKIPPED   {err}")
        continue
    hashes = [hashlib.sha256(o.encode()).hexdigest()[:10] for o in outs]
    identical = len(set(hashes)) == 1
    lens = [len(o) for o in outs]
    results[prov] = identical
    print(f"  {prov:<14} {'IDENTICAL  ' if identical else 'VARIES     '} "
          f"hashes={hashes} lens={lens}")

print()
stable = [p for p, ok in results.items() if ok]
print("Reproducible providers:", stable if stable else "NONE")
