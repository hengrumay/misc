---
name: misc-project-check
description: >-
  Onboard a new example into the `misc` repo of synthetic-data projects, and run the
  pre-share safety check that keeps that repo publishable. Use this whenever someone is
  adding, scaffolding, or preparing a project folder under `misc/projects/`, and BEFORE
  committing, pushing, sharing, or publishing any misc project — and whenever asked
  "is this safe to share?", "does this have PII or customer references?", "scrub this
  before it goes public", or "onboard this into misc". Do not skip it because a project
  "looks clean": the whole point is catching the customer fingerprints that survive a
  casual read — brand colors, internal org URLs, project codenames, workspace hosts,
  and resource IDs.
---

# misc-project-check

The `misc` repo shares **synthetic-data examples** in the open. Its one
non-negotiable rule: nothing customer-identifying, no personal data (PII), no
internal hosts or IDs, no secrets. This skill covers the two moments where that
rule gets enforced:

- **Onboard** — add a new project from the `_template/` accelerator.
- **Check** — the safety gate a project must pass before it is committed or shared.

Onboarding a project is not finished until it passes the Check. Treat the Check
as the gate, not an afterthought — a public repo is unforgiving, and the leaks
that matter are the ones a quick read misses.

---

## Onboard a new project

Every project starts from the `_template/` accelerator. The template's shape is:

```
_template/
├── README.md         # project README placeholder — fill it in (Purpose / Prerequisites / How to run / Synthetic-data note)
├── databricks.yml    # Databricks Asset Bundle definition
├── notebooks/        # example notebooks
├── apps/             # Databricks Apps (optional)
├── dashboards/       # AI/BI dashboards (optional)
├── scripts/          # setup / run helpers
├── env.example       # environment-variable template
└── requirements.txt  # Python dependencies
```

A project's **internal layout is flexible and example-driven — it is not
mandated.** Start from the template and add whatever the example needs. For
instance, the `rwe-ads-reference` project uses `lib/`, `waves/`, `pipelines/`,
`app/`, `tests/`, and `docs/` instead of the bare template folders.

Steps:

1. Copy the template and rename it: `cp -r _template projects/<name>`
2. Fill in `projects/<name>/README.md` (the copied placeholder) — what it
   demonstrates, prerequisites, and how to run it, plus a synthetic-data note, in
   plain language. Write it for a stranger: no customer context, no "as we saw at
   <client>", no internal environment names.
3. Add the project's code and its **synthetic** data generator. Keep every data
   file generated / fake. Shape the internals however the example needs (see above).
4. Add a row to the **Projects** table in the top-level `README.md`.
5. Run the **Check** below. Fix every HIGH and MEDIUM finding, then re-run until
   clean. Only then is the project ready to commit.

---

## Check — the pre-share safety gate

Run from the repo root or a single project. Set the target once:

```bash
DIR=projects/<name>     # or "." to scan the whole repo
```

Prefer `rg` (ripgrep) for speed; if it is not installed, the same patterns work
with `grep -rnE`. Each category below says what it catches and how bad a hit is.

### 1. Secrets / tokens — HIGH
```bash
rg -n -i -e "dapi[0-9a-f]{16,}|-----BEGIN [A-Z ]*PRIVATE KEY|xox[baprs]-|AKIA[0-9A-Z]{16}|ghp_[0-9A-Za-z]{36}|(password|secret|api[_-]?key|token)\s*[:=]\s*[^<\s]" "$DIR"
```
Placeholders like `<your-token>` or `DATABRICKS_TOKEN=<your-PAT>` are fine — eyeball
each hit. Any real-looking value must go.

### 2. Internal hosts / workspaces — HIGH
```bash
rg -n -i -e "adb-[0-9]+\.[0-9]+\.azuredatabricks\.net|dbc-[0-9a-f-]+\.cloud\.databricks\.com|[a-z0-9-]+\.cloud\.databricks\.com" "$DIR" | rg -v "your-workspace|<your"
```
A concrete host (anything that is not a `<placeholder>`) is a leak. Auth should be
profile-based; the host should never be hardcoded.

### 3. Storage / connection URIs — HIGH
```bash
rg -n -e "s3://|abfss://|gs://|wasbs://|\.dfs\.core\.windows\.net|jdbc:|postgres://" "$DIR"
```

### 4. Emails / handles — HIGH if real
```bash
rg -n -i -e "[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}" "$DIR" | rg -v "example\.com|<your"
```
Everything real (especially `@databricks.com`) gets replaced with `<your-email>` or
`someone@example.com`.

### 5. Resource IDs — MEDIUM/HIGH
```bash
rg -n -e "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" "$DIR"   # UUIDs
rg -n -e "\b[0-9]{12}\b" "$DIR"                                                   # 12-digit account IDs
rg -n -e "\b[0-9a-f]{16}\b" "$DIR"                                                # 16-hex warehouse IDs
```
Ignore hits inside compiled/minified assets (`app/static`, `*.min.js`,
`package-lock.json`) — those are false positives.

### 6. Internal orgs / repos / codenames — MEDIUM
```bash
rg -n -i -e "databricks-field-eng|field-eng|internal only|do not distribute|for internal review|before (any )?external use" "$DIR"
```
Also watch for your own project's internal codenames — keep those in the private
denylist (category 9) so the scan flags them.

### 7. Brand fingerprints — HIGH (the sneaky one)
A customer's identity often outlives their *name*: it survives as brand **colors**
or a two-letter key in a `branding:` / theme block long after every literal mention
is gone. A real audit of a bundle that had zero name hits still found a customer's
corporate palette wired into the app and served to the frontend. So inspect any
branding/theme/color config:
```bash
rg -n -i -e "brand|branding|palette|theme|accent|#[0-9a-fA-F]{6}" "$DIR" -g "*.yaml" -g "*.yml" -g "*.json" -g "*.ts" -g "*.tsx" -g "*.css"
```
Then judge every hex color: is it a Databricks / neutral color, or a customer's
corporate color? If you cannot prove a hex is generic, replace it with a neutral
placeholder. Also grep the prose docs for a hex you just removed — the same color
often gets mislabeled in a README.

### 8. Committed real data — HIGH
```bash
find "$DIR" -type f \( -name "*.csv" -o -name "*.parquet" -o -name "*.json" -o -name "*.jsonl" \) -not -path "*/node_modules/*"
```
Open each one: is it generator-produced/synthetic, or a real extract? Only synthetic
data — or the generator that makes it — belongs here. Never commit a real dataset.

### 9. Customer names — private denylist — HIGH
NEVER hardcode a customer's name (or their codenames/palette) into this repo or this
skill — that would publish the very thing you are scrubbing. Instead keep a private,
git-ignored denylist that lives **outside** the repo, one term per line, and scan
against it:
```bash
DENYLIST=~/.misc-scrub-denylist.txt      # customer names, product codenames, brand hex, employee handles
rg -n -i -f "$DENYLIST" "$DIR"
```
Seed it with the customer and engagement this project came from. Because the file
lives outside the repo, it never ships.

---

## Severity and the gate

- **HIGH** — customer name, PII, secret, real host, real resource ID, brand
  fingerprint, or real data. Must be fixed before the project is committed.
- **MEDIUM** — internal org/repo/codename, workspace-specific config values.
  Must be fixed before the project is shared.
- **LOW** — "internal review / before external use" doc markers, a secret-scope
  *path* name (no value), sibling internal-project mentions. Polish.

**Gate:** zero HIGH and zero MEDIUM hits (outside known `<placeholder>` values) →
the project is safe to commit and share. After any fix, re-run the relevant category
and confirm the hit is gone — a scrub is not done until the grep comes back empty.

---

## What "clean" looks like

- Hosts are `<your-workspace>.cloud.databricks.com`; auth via a Databricks CLI
  profile, never a hardcoded host.
- Catalog / schema / endpoint names are generic (`<project>_catalog`,
  `<project>-endpoint`) with a "rename with your initials" note.
- Emails are `@example.com` or `<your-email>`.
- Config files carry only placeholders (`<your-...>`); `.env*`, `*.pem`, `*.key`,
  and any real `*.config.yaml` are git-ignored.
- Any sample/reference data is synthetic and fictional — no real study, trial,
  drug program, or customer identifiers.

## Notes

- If `rg` is missing, use `grep -rnE` with the same patterns.
- Compiled/minified bundles (`app/static/assets/*`, `*.min.*`, `package-lock.json`)
  produce false positives on the ID and hex checks — confirm a hit is real content,
  not a minifier artifact, before flagging it.
