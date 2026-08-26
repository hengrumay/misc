# misc

A monorepo of small, self-contained **synthetic-data example projects** for
Databricks. Each project under [`projects/`](projects/) is a standalone example
you can read, run, and share.

> **Synthetic only.** Everything in this repo uses generated / fake data.
> Nothing here may contain:
> - customer or company names (real engagements),
> - personal data (PII / PHI — real names, dates of birth, contact details),
> - internal workspace URLs, workspace IDs, account IDs, or resource IDs,
> - secrets, tokens, or credentials (placeholders like `<your-token>` are fine),
> - proprietary brand colors / brand hex codes or other brand fingerprints.
>
> Read **[Adding a project](#adding-a-project)** and run the scrub check before
> you commit anything.

## Projects

| Project | What it shows |
|---------|---------------|
| [rwe-ads-reference](projects/rwe-ads-reference/) | Real-world-evidence (RWE) → analysis-data-set (ADS) reference on Databricks: synthetic patients / claims → medallion pipelines → served ADS app, with protected-health-information (PHI) masking, protocol extraction, and a review-and-sign-off gate. |

## Layout

```
misc/
├── _template/          # starter accelerator — copy it to begin a new project
│   ├── README.md       #   project README placeholder (fill it in)
│   ├── databricks.yml  #   Databricks Asset Bundle definition
│   ├── notebooks/  apps/  dashboards/  scripts/
│   ├── env.example
│   └── requirements.txt
├── projects/           # one folder per example project
│   └── <name>/         # self-contained; internal layout is flexible (see below)
├── .github/            # shared CI / publish workflows
├── LICENSE.md  NOTICE.md  SECURITY.md  CONTRIBUTING.md
└── README.md           # this index
```

Each project starts from `_template/` and is self-contained — it carries its own
README (what it is + how to run it) plus everything needed to generate its
synthetic data. A project's **internal layout is flexible**, not mandated: some
projects use the bare template folders (`notebooks/`, `scripts/`, …), while
`rwe-ads-reference` uses `lib/`, `waves/`, `pipelines/`, `app/`, `tests/`, and
`docs/`. The project's own README is the entry point.

## Adding a project

1. Copy the starter template and rename it:
   `cp -r _template projects/my-example`
2. Fill in `projects/my-example/README.md` — the template ships a placeholder
   ([`_template/README.md`](_template/README.md)) with **Purpose / Prerequisites /
   How to run / Synthetic-data note**. Complete it in plain language.
3. Add your code and a **synthetic** data generator. Generated / fake data only;
   shape the project's internals however the example needs.
4. Add a row to the **Projects** table above with a one-line description.
5. **Run the scrub check before committing.** The `misc-project-check` skill
   (in [`.claude/skills/misc-project-check/`](.claude/skills/misc-project-check/SKILL.md))
   walks the onboarding steps and scans for anything that breaks the
   synthetic-only rule above — customer names, PII/PHI, internal URLs / IDs,
   secrets, and brand fingerprints. Fix every hit before you commit.

Synthetic only. When in doubt, leave it out.
