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
├── _template/          # starter accelerator template — copy this to begin a new project
├── projects/           # one folder per example project
│   └── <name>/         # self-contained: its own README, code, and synthetic-data generator
├── .github/            # shared CI / publish workflows
├── LICENSE.md  NOTICE.md  SECURITY.md  CONTRIBUTING.md
└── README.md           # this index
```

Each project is self-contained — it carries its own README explaining what it is
and how to run it, plus everything needed to generate its synthetic data. Project
internals vary (some use notebooks, some use Databricks Asset Bundles with
pipelines and an app); the project's own README is the entry point.

## Adding a project

1. Copy the starter template and rename it:
   `cp -r _template projects/my-example`
2. Fill in `projects/my-example/README.md` — purpose, prerequisites, how to run.
3. Add the synthetic-data generator and code. Generated / fake data only.
4. Add a row to the **Projects** table above with a one-line description.
5. **Run the scrub check before committing.** The `misc-project-check` skill
   (in [`.claude/skills/misc-project-check/`](.claude/skills/misc-project-check/SKILL.md))
   walks the onboarding steps and scans for anything that breaks the
   synthetic-only rule above — customer names, PII/PHI, internal URLs / IDs,
   secrets, and brand fingerprints. Fix every hit before you commit.

Synthetic only. When in doubt, leave it out.
