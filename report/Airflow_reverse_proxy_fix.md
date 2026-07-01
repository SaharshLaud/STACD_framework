# Airflow behind external reverse proxy — path prefix fix

## Part 1 — Brief summary

**Problem:** `https://www.cse.iitd.ernet.in/act4dws5/airflow/` loaded the Airflow
dashboard, but every redirect Airflow generated (login, home, static assets)
dropped the `/act4dws5` prefix. The browser then resolved that relative
redirect against `www.cse.iitd.ernet.in` directly, landed on a path bahar
doesn't route, and fell through to the CSE department homepage instead of
Airflow.

**Cause:** Two reverse proxy layers sit in front of Airflow —
`bahar` (`www.cse.iitd.ernet.in/act4dws5/...`) → `corestack-nginx`
(`act4dws5.cse.iitd.ac.in/...`) → the Airflow container (`/airflow/...`).
`bahar` strips `/act4dws5` before forwarding, so Airflow only ever *received*
requests at `/airflow/...` and had no way to know a `/act4dws5` prefix existed
one layer further out. Its `base_url` config — which controls both route
matching and redirect generation — could only be correct for one of those two
jobs at a time, not both, unless the actual incoming request path was made to
match what `base_url` expected.

**Fix:** Set `AIRFLOW__WEBSERVER__BASE_URL` to the full external URL
(`https://www.cse.iitd.ernet.in/act4dws5/airflow`), and changed
`corestack-nginx`'s `/airflow/` proxy target so the request forwarded to the
Airflow container itself carries the `/act4dws5/airflow` prefix, matching what
`base_url` now expects. `AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True` was also
enabled.

**Result:** `https://www.cse.iitd.ernet.in/act4dws5/airflow/` and
`http://act4dws5.cse.iitd.ac.in/airflow/` both now redirect correctly to
`/act4dws5/airflow/home`, staying on the right domain and path.

**Known separate, still-open issue:** the session cookie's `Path` differs
between direct and via-bahar requests (`Path=/` vs `Path=/act4d/`) —
confirmed to be injected/rewritten by `bahar` itself, unaffected by this fix.
This can still break login persistence through the external URL and needs a
fix on bahar's side (owned by CSE networking, not this stack) — tracked
separately.

---

## Part 2 — Detailed explanation

### Architecture

```
Browser
  │
  ▼
bahar (Apache, www.cse.iitd.ernet.in)
  — maps /act4dws5/<path> → act4dws5.cse.iitd.ac.in/<path>, stripping /act4dws5
  │
  ▼
corestack-nginx (Docker container, act4dws5.cse.iitd.ac.in)
  — location /airflow/ → proxies to Airflow container
  │
  ▼
Airflow webserver (Flask-AppBuilder, inside corestac-stacd-airflow container)
```

`bahar` strips its own routing prefix before forwarding — this is why the
request that physically reaches the workstation (and therefore Airflow) never
contains `/act4dws5`. Confirmed directly: a request sent to
`http://www.cse.iitd.ernet.in/act4dws5/airflow` (no trailing slash) produced a
`301 Location: https://www.cse.iitd.ernet.in/act4dws5//airflow/` — the
external host and `/act4dws5` prefix intact — proving that redirect came from
bahar itself, which is the only layer aware of that hostname/prefix.

### Why `base_url` alone can't fix this

Airflow's `AIRFLOW__WEBSERVER__BASE_URL` config is used for two distinct
purposes inside Flask-AppBuilder:

1. **Route registration / matching** — the path segment of `base_url`
   determines what URL path Airflow's Flask app actually listens on. An
   incoming request must match this path exactly for Airflow to serve it.
2. **Outbound URL generation** — the same config is used to build the
   `Location:` header on redirects (login, home) and links inside the UI.

These two jobs pull in opposite directions here:

- If `base_url`'s path is set to the *internal* path (`/airflow`, matching
  what actually arrives after bahar strips the prefix) — route matching
  works, but every generated redirect is also just `/airflow/...`, missing
  `/act4dws5`. This was the **original bug**.
- If `base_url`'s path is set to the *external* path (`/act4dws5/airflow`) —
  generated redirects would be correct, but Flask-AppBuilder now expects
  every incoming request to already carry `/act4dws5/airflow`. Since bahar
  strips that prefix before the request ever reaches the workstation, no
  request matches, and Airflow returns its own 404 page,
  `"Apache Airflow is not at this location"`. This was proven directly by
  setting `base_url` to the external URL in isolation, without any nginx
  change — result was a clean, single-hop `404` (confirmed via
  `curl -v`, one `GET`/one `HTTP` line, no redirect chain, Airflow's own
  error text in the response body, not nginx's or bahar's).

### The two approaches considered

**Approach A — `X-Forwarded-Prefix` (attempted first, not what's live):**
Keep `base_url`'s path as the *internal* path, enable
`AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True`, and have `corestack-nginx` send
`proxy_set_header X-Forwarded-Prefix /act4dws5/airflow;` on the `/airflow/`
block. Werkzeug's `ProxyFix` middleware then uses this header, on a
per-request basis, to correct only the *outbound* URL generation, leaving
route matching against the internal path untouched. This is the standard
pattern for apps behind path-rewriting proxies and was verified to be
necessary in principle — but a **pre-existing, stale** `X-Forwarded-Prefix
/airflow;` line was already present in `geoserver.conf` from an earlier setup
(harmless while `ENABLE_PROXY_FIX` was `False`, since it was never read) and,
even after correcting its value, this approach was ultimately abandoned in
favor of Approach B during troubleshooting.

**Approach B — path-rewriting `proxy_pass` (what's actually deployed):**
Set `base_url` to the **full external URL**, and instead of relying on
`X-Forwarded-Prefix`, change what `corestack-nginx` forwards to the Airflow
container so the *request path itself* already contains `/act4dws5/airflow`
by the time it reaches Flask-AppBuilder. This makes the incoming request path
match what `base_url` expects for route registration, and since `base_url`
now also holds the full external URL, outbound redirect generation is
correct too — both jobs are satisfied by the same value, because the request
path was changed to match it, rather than told about it via a header.

### Final configuration (confirmed live)

**`~/deployment/stacd/.env`:**
```
AIRFLOW__WEBSERVER__BASE_URL=https://www.cse.iitd.ernet.in/act4dws5/airflow
AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
CATALOG_BASE_URL=https://www.cse.iitd.ernet.in/act4dws5/stacd
```

**`~/deployment/nginx/geoserver.conf`, `location /airflow/` block:**
```nginx
location /airflow/ {
    proxy_pass http://corestac-stacd-airflow:8080/act4dws5/airflow/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```
Note the `proxy_pass` target now ends in `/act4dws5/airflow/` rather than
`/airflow/` — this is what rewrites the forwarded request path so it matches
`base_url`. No `X-Forwarded-Prefix` header is set in the final version; it
isn't needed under this approach.

The Airflow container was recreated (not just restarted) after the `.env`
change, since `docker restart` does not re-read the host `.env` file —
required for the new environment variables to actually take effect.

### Verification

```bash
curl -sI "https://www.cse.iitd.ernet.in/act4dws5/airflow/"
# HTTP/1.1 302 FOUND
# Location: /act4dws5/airflow/home        ← prefix preserved

curl -sI "http://act4dws5.cse.iitd.ac.in/airflow/"
# HTTP/1.1 302 FOUND
# Location: /act4dws5/airflow/home        ← same, direct access unaffected

curl -sI "https://www.cse.iitd.ernet.in/act4dws5/airflow/login/"
# HTTP/1.1 200 OK
```
Both external and direct-to-workstation access now redirect correctly and
land on Airflow's login/home pages rather than being bounced to the CSE
homepage.

### Remaining known issue (out of scope for this fix)

```
Direct to workstation:  Set-Cookie: session=...; Path=/
Via bahar:              Set-Cookie: session=...; Path=/act4d/
```
Same request, same Airflow instance — the only difference is which hop it
went through. The `/act4d/` value does not match `/act4dws5/airflow/` at all,
so this cookie will never be sent back by the browser through the external
URL, meaning login sessions will not persist there even though the redirect
itself is now correct. Confirmed via direct comparison that Airflow emits
`Path=/` (Flask's own default; nothing in `airflow.cfg` or
`webserver_config.py` sets a custom cookie path) and `corestack-nginx` does
not modify `Set-Cookie` headers anywhere in its config — the `/act4d/`
value is added between `corestack-nginx` and the browser, i.e. by `bahar`.
This needs to be raised with CSE networking (bahar's owner) separately, since
neither this stack's Airflow config nor `corestack-nginx` can correct it.

### Note — unrelated possible issue spotted during this investigation

While confirming the final `geoserver.conf`, the `/js/`, `/css/`, `/img/`,
and `/fonts/` blocks (which serve STAC Browser static assets on port 8082,
unrelated to Airflow) were found with `/act4dws5/airflow/` appended into
their proxy target:
```nginx
location /js/ {
    set $stac_js http://corestac-stacd-airflow:8082/act4dws5/airflow/;
    proxy_pass $stac_js/js/;
}
```
This was not an intentional part of this fix and looks like it may have come
from a broad find-replace during editing. Worth checking separately — this
would resolve to a path that almost certainly doesn't exist on the STAC
Browser and could break its static asset loading.
