# Reference frameworks

Pre-authored documents describing an external, authoritative taxonomy of
insurance business domains. These files anchor the LLM-assisted taxonomy
research (`scripts/research_domain_taxonomy.py`) so that proposed domains
map to a defensible industry reference rather than being invented.

## Format

Each framework lives in its own markdown file (e.g. `lloyds_mdc.md`). The
name of the file (minus extension) is the `--reference` flag value accepted
by the research script.

Recommended structure for a framework file:

```markdown
# <Framework name>

**Source:** <org, publication, URL>
**Version / year:** <when published>
**Scope:** <specialty / P&C / life / reinsurance / ...>

## Value chain (or top-level domains)

1. <Domain 1> — <one-sentence scope>
2. <Domain 2> — ...

## Definitions

### <Domain 1>

<Two or three paragraphs. What activities belong here. What does NOT
belong here. Adjacent domains and how they differ. Typical data artefacts.>

### <Domain 2>

...

## Known overlaps / edge cases

- <Domain A> vs <Domain B>: <how the framework draws the line>
```

Keep these terse and source-anchored. Do not fabricate content — if a
framework section isn't public or you don't have the authoritative wording,
leave a placeholder and cite the source for human follow-up. The research
script passes these verbatim into the prompt as grounding; bad grounding
produces bad research.

## Adding a new framework

1. Drop `<your_framework>.md` in this directory.
2. Follow the structure above.
3. Run: `python scripts/research_domain_taxonomy.py --bundle output/bundle.json --reference <your_framework>`.

## Available frameworks

- `lloyds_mdc.md` — placeholder; populate before running research against it.
