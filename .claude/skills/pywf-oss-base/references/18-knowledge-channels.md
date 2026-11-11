---
description: "Agent-first knowledge: the three channels a fact can go to (Sphinx user docs, the pypi-<package_name> API skill, the maintain-<package_name> skill family), which fact goes where, and the shape of the maintenance skill family. Read when deciding where to write down how to use or how to maintain the library, or before writing a maintainer or contributor guide."
---

# Knowledge Channels

A library is read by people and, more and more, by agents: agents that use it in other projects, and agents that maintain it here. So each fact goes to exactly one channel, chosen by who needs it to act.

---

## 1. The three channels

| Channel | Reader | Where | Written by |
| :--- | :--- | :--- | :--- |
| **User docs** | People learning to use the library | `docs/source/` (Sphinx, `13-sphinx.md`), hosted on Read the Docs | `pywf-oss-write-doc` |
| **API skill** | An agent using the library in another project | `.claude/skills/pypi-<package_name>/` | `pywf-oss-write-pypi-skill` |
| **Maintenance skill family** | An agent (or person) changing this repo | `.claude/skills/<maintain_prefix>-*/` | `pywf-oss-write-maintain-skill` |

- **User docs** teach the library: guides, concepts, runnable notebooks, and the generated API reference.
- **The API skill** mirrors `api.py` (`08-package-files.md` section 1), and is regenerated after each release that changes the public API.
- **There is no maintainer guide in the docs.** How this repo is maintained lives in the maintenance skill family, which an agent loads when it works here, and which a human contributor can read as plain Markdown on GitHub.

---

## 2. Which fact goes where

| Fact | Channel |
| :--- | :--- |
| What the library does, how to install it, a short example | `README.rst` |
| A tutorial, a concept, a runnable notebook | User docs |
| Every public name: signature, behavior, a short example; the CLI | API skill |
| A template convention shared by every pywf repo (layout, tests, release) | `pywf-oss-base`; never repeated per repo |
| This library's own architecture, invariants, design decisions, and why | Maintenance family, base references |
| A repeatable job specific to this library ("add a backend", "regenerate the fixtures", "refresh the vendored data") | Maintenance family, one invoker per job |
| Debugging recipes, the external services it talks to, known traps | Maintenance family, base references |

When a fact would fit two channels, it belongs to the one whose reader needs it to act.

---

## 3. The maintenance skill family

It follows the same base and invoker pattern as the `pywf-oss` family (`00-authoring-references.md` section 1), with the prefix `<maintain_prefix>` (`maintain-<package_name>`):

```text
.claude/skills/
├── <maintain_prefix>-base/
│   ├── SKILL.md            # index of its references, scripts, and assets; which invoker does what
│   ├── references/         # NN-slug.md, numbered, cite backward only
│   ├── scripts/            # optional, stdlib-only, same conventions as pywf-oss-base scripts
│   └── assets/             # optional
└── <maintain_prefix>-<action>/
    └── SKILL.md            # one job: "Read ../<maintain_prefix>-base/SKILL.md, then: ..."
```

- **Only what is specific to this library.** A template convention is cited by skill name ("load `pywf-oss-base`, then read `10-test-layout.md`"), never copied.
- **Start with the base alone.** A small library needs only a base with a few references. Add an invoker when a job specific to this library repeats.
- **Write it from the code,** not from memory: every claim names the file or function it describes, so a later reader can check it.
- **Keep it current.** A change that alters an invariant or a maintenance job updates the family in the same commit.
- It is committed with the repo, and never part of the docs build.
