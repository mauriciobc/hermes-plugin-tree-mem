#!/usr/bin/env python3
"""
llm_call.py — one-shot LLM completion with provider fallback for Hermes cron jobs.

Order: opencode-zen free-tier models break often (announced retirements, FreeTierError
storms); opencode-go deepseek-v4.1-flash is the paid-by-subscription fallback.

Usage:
  llm_call.py <model> <provider> <prompt> [--max-tokens N] [--fallback]
  llm_call.py space-bunny-free opencode-zen "Compress: ..." --fallback

Env: OPENCODE_ZEN_API_KEY / OPENCODE_GO_API_KEY must be in the process env
(gateway processes export both).

Exit 0 prints the content text. On total failure prints nothing, exits 1.
"""
import argparse, json, os, sys, urllib.request, urllib.error, uuid

PROVIDERS = {
    "opencode-zen": {"base": "https://opencode.ai/zen/v1", "env": "OPENCODE_ZEN_API_KEY", "needs_session": False},
    "opencode-go":  {"base": "https://opencode.ai/zen/go/v1", "env": "OPENCODE_GO_API_KEY", "needs_session": True},
}
# Verified working 2026-10-21: deepseek-v4.1-flash and glm-5.3-flash on opencode-go.
# opencode-zen's FREE tier is closed to raw API calls (FreeTierError) and its
# free models are deprecated on a rolling basis (HTTP 410) — do not rely on it.
FALLBACK = ("deepseek-v4.1-flash", "opencode-go")

def call(model, provider, prompt, max_tokens=1200):
    p = PROVIDERS[provider]
    tok = os.environ.get(p["env"], "")
    if not tok:
        # Hermes resolves provider secrets from ~/.hermes/.env at CALL time —
        # the gateway process env can hold a stale copy. Prefer the dotenv.
        try:
            from hermes_cli.config import get_env_value_prefer_dotenv
            tok = (get_env_value_prefer_dotenv(p["env"]) or "").strip()
        except Exception:
            pass
    if not tok:
        # try the gateway proc env (cron can run outside it)
        try:
            for pid in os.listdir('/proc'):
                if not pid.isdigit(): continue
                try: en=open(f'/proc/{pid}/environ','rb').read().decode(errors='replace').split(chr(0))
                except Exception: continue
                hit=[e.split('=',1)[1] for e in en if e.startswith(p["env"]+'=')]
                if hit: tok=hit[0]; break
        except Exception:
            pass
    if not tok:
        raise RuntimeError(f'no credential for {provider} ({p["env"]})')
    hdr={'Authorization':'Bearer '+tok,'Content-Type':'application/json','Accept':'application/json',
         'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126 Safari/537.36'}
    if p["needs_session"]:
        hdr['x-opencode-session']='hermes-cron-'+uuid.uuid4().hex[:12]
    body=json.dumps({'model':model,'max_tokens':max_tokens,
                     'messages':[{'role':'user','content':prompt}]}).encode()
    req=urllib.request.Request(p["base"]+'/chat/completions', data=body, headers=hdr)
    r=urllib.request.urlopen(req, timeout=120)
    d=json.loads(r.read())
    return d['choices'][0]['message'].get('content') or ''

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('model'); ap.add_argument('provider'); ap.add_argument('prompt')
    ap.add_argument('--max-tokens', type=int, default=1200)
    ap.add_argument('--fallback', action='store_true')
    a=ap.parse_args()
    try:
        out=call(a.model, a.provider, a.prompt, a.max_tokens)
        if out.strip():
            print(out); return 0
    except Exception as e:
        sys.stderr.write(f'primary {a.provider}/{a.model}: {type(e).__name__} {str(e)[:120]}\n')
    if a.fallback:
        out=call(*FALLBACK, a.prompt, a.max_tokens)
        if out.strip():
            print(out); return 0
        sys.stderr.write('fallback also failed\n')
    return 1

if __name__=='__main__':
    sys.exit(main())
